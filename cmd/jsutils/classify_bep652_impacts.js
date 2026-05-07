import { readFileSync, writeFileSync } from "fs";

const EIP1967_IMPLEMENTATION_SLOT =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
const EIP1967_BEACON_SLOT =
    "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50";
const EIP1967_ADMIN_SLOT =
    "0xb53127684a568b3173ae13b9f8a6016e019a4a4f7f1f2f7a7e3f6a6b5a2e7b8c";

function parseArgs(argv) {
    const args = {
        summary: "",
        out: "",
        rpc: "",
    };
    for (let i = 2; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === "-h" || arg === "--help") {
            args.help = true;
            continue;
        }
        if (!arg.startsWith("--")) {
            throw new Error(`unexpected argument: ${arg}`);
        }
        const key = arg.slice(2);
        const value = argv[++i];
        if (value === undefined || value.startsWith("--")) {
            throw new Error(`missing value for ${arg}`);
        }
        args[key] = value;
    }
    return args;
}

function usage() {
    console.log("Usage:");
    console.log("  node classify_bep652_impacts.js --summary <summary.json> [--rpc <rpc>] [--out <report.json>]");
    console.log("");
    console.log("Examples:");
    console.log(
        "  node classify_bep652_impacts.js --summary cmd/jsutils/bep652-first-pass-95090937-96660845.summary.json --rpc https://bsc-mainnet.nodereal.io/v1/<key>"
    );
}

function requireArg(value, name) {
    if (!value) {
        throw new Error(`missing required argument ${name}`);
    }
}

function defaultOutPath(summaryPath) {
    if (summaryPath.endsWith(".summary.json")) {
        return summaryPath.replace(".summary.json", ".impact-report.json");
    }
    if (summaryPath.endsWith(".json")) {
        return summaryPath.replace(".json", ".impact-report.json");
    }
    return `${summaryPath}.impact-report.json`;
}

function fromHexToBigInt(hexValue) {
    if (!hexValue || hexValue === "0x") {
        return 0n;
    }
    return BigInt(hexValue);
}

function toEthString(weiBigInt) {
    const eth = Number(weiBigInt) / 1e18;
    return eth.toFixed(6);
}

class RpcClient {
    constructor(url) {
        this.url = url;
        this.id = 1;
    }

    async call(method, params) {
        const res = await fetch(this.url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                jsonrpc: "2.0",
                id: this.id++,
                method,
                params,
            }),
        });
        if (!res.ok) {
            throw new Error(`HTTP ${res.status} ${res.statusText}`);
        }
        const body = await res.json();
        if (body.error) {
            throw new Error(`${body.error.code}: ${body.error.message}`);
        }
        return body.result;
    }

    async getCode(address) {
        return this.call("eth_getCode", [address, "latest"]);
    }

    async getBalance(address) {
        return this.call("eth_getBalance", [address, "latest"]);
    }

    async getStorageAt(address, slot) {
        return this.call("eth_getStorageAt", [address, slot, "latest"]);
    }
}

function summarizeByContract(groups) {
    const contracts = new Map();
    for (const group of groups) {
        const entry = contracts.get(group.to) || {
            contract: group.to,
            selectors: [],
            failedTxCount: 0,
            likelyRevertCount: 0,
            oogLikeCount: 0,
            gasUsedAboveCapCount: 0,
            unrecoveredSelectorCount: 0,
            recoveredSelectorCount: 0,
            firstBlock: group.firstBlock,
            lastBlock: group.lastBlock,
        };

        entry.selectors.push(group.selector);
        entry.failedTxCount += group.failedCount || 0;
        entry.likelyRevertCount += group.likelyRevertCount || 0;
        entry.oogLikeCount += group.oogLikeCount || 0;
        entry.gasUsedAboveCapCount += group.gasUsedAboveCapCount || 0;
        entry.firstBlock = Math.min(entry.firstBlock, group.firstBlock);
        entry.lastBlock = Math.max(entry.lastBlock, group.lastBlock);
        const recovered = (group.recovery?.sameSelectorSuccessAfterFailure?.count || 0) > 0;
        if (recovered) {
            entry.recoveredSelectorCount++;
        } else {
            entry.unrecoveredSelectorCount++;
        }

        contracts.set(group.to, entry);
    }
    return Array.from(contracts.values());
}

function scoreCriticalPath(contract) {
    if (contract.unrecoveredSelectorCount > 0 && contract.failedTxCount >= 5) {
        return { level: "high", reason: "multiple failed txs and unrecovered selector(s)" };
    }
    if (contract.unrecoveredSelectorCount > 0 || contract.failedTxCount >= 3) {
        return { level: "medium", reason: "either unrecovered selector or repeated failures" };
    }
    return { level: "low", reason: "few failures and selector(s) recovered" };
}

function scoreFundsLock(contract, nativeBalanceWei) {
    const hasBalance = nativeBalanceWei > 0n;
    if (contract.unrecoveredSelectorCount > 0 && hasBalance) {
        return { level: "high", reason: "unrecovered selector and contract still holds native balance" };
    }
    if (contract.unrecoveredSelectorCount > 0 || hasBalance) {
        return { level: "medium", reason: "either unrecovered selector or non-zero native balance" };
    }
    return { level: "low", reason: "no unrecovered selector and zero native balance" };
}

function scoreUpgradability(proxyInfo, contract) {
    if (proxyInfo.proxyDetected) {
        return { level: "low", reason: "proxy-like contract; upgrade path likely exists" };
    }
    if (contract.unrecoveredSelectorCount > 0) {
        return { level: "high", reason: "non-proxy-like and unrecovered selector remains" };
    }
    return { level: "medium", reason: "non-proxy-like but selectors recovered" };
}

async function enrichContract(rpc, contractAddr) {
    if (!rpc) {
        return {
            nativeBalanceWei: 0n,
            nativeBalanceEth: "0.000000",
            codeSize: 0,
            proxyDetected: false,
            proxySignal: "rpc_not_provided",
        };
    }

    const [code, balanceHex, implSlot, beaconSlot, adminSlot] = await Promise.all([
        rpc.getCode(contractAddr),
        rpc.getBalance(contractAddr),
        rpc.getStorageAt(contractAddr, EIP1967_IMPLEMENTATION_SLOT),
        rpc.getStorageAt(contractAddr, EIP1967_BEACON_SLOT),
        rpc.getStorageAt(contractAddr, EIP1967_ADMIN_SLOT),
    ]);

    const implSet = fromHexToBigInt(implSlot) !== 0n;
    const beaconSet = fromHexToBigInt(beaconSlot) !== 0n;
    const adminSet = fromHexToBigInt(adminSlot) !== 0n;
    const proxyDetected = implSet || beaconSet || adminSet;

    const nativeBalanceWei = fromHexToBigInt(balanceHex);
    return {
        nativeBalanceWei,
        nativeBalanceEth: toEthString(nativeBalanceWei),
        codeSize: code && code.startsWith("0x") ? (code.length - 2) / 2 : 0,
        proxyDetected,
        proxySignal: proxyDetected ? "eip1967_slot_set" : "no_eip1967_slot_signal",
    };
}

async function main() {
    const args = parseArgs(process.argv);
    if (args.help || process.argv.length <= 2) {
        usage();
        return;
    }

    requireArg(args.summary, "--summary");
    const outPath = args.out || defaultOutPath(args.summary);

    const summary = JSON.parse(readFileSync(args.summary, "utf8"));
    const contractSummaries = summarizeByContract(summary.groups || []);
    const rpc = args.rpc ? new RpcClient(args.rpc) : null;

    const contractReports = [];
    for (const c of contractSummaries) {
        const enrichment = await enrichContract(rpc, c.contract);
        const upgradability = scoreUpgradability(enrichment, c);
        const criticalPath = scoreCriticalPath(c);
        const fundsLock = scoreFundsLock(c, enrichment.nativeBalanceWei);

        contractReports.push({
            contract: c.contract,
            failedTxCount: c.failedTxCount,
            selectorCount: c.selectors.length,
            selectors: c.selectors,
            unrecoveredSelectorCount: c.unrecoveredSelectorCount,
            recoveredSelectorCount: c.recoveredSelectorCount,
            likelyRevertCount: c.likelyRevertCount,
            oogLikeCount: c.oogLikeCount,
            gasUsedAboveCapCount: c.gasUsedAboveCapCount,
            firstBlock: c.firstBlock,
            lastBlock: c.lastBlock,
            onchain: {
                nativeBalanceEth: enrichment.nativeBalanceEth,
                codeSize: enrichment.codeSize,
                proxyDetected: enrichment.proxyDetected,
                proxySignal: enrichment.proxySignal,
            },
            impactLevels: {
                nonUpgradeable: upgradability,
                criticalPath,
                userFundsLock: fundsLock,
            },
        });
    }

    contractReports.sort((a, b) => b.failedTxCount - a.failedTxCount);

    const report = {
        meta: {
            sourceSummary: args.summary,
            generatedAt: new Date().toISOString(),
            rpc: args.rpc || null,
            note: "Classification is heuristic. Validate with trace and contract-specific business context.",
        },
        totals: {
            affectedContracts: contractReports.length,
            failedTxCount: contractReports.reduce((sum, c) => sum + c.failedTxCount, 0),
        },
        impactedContracts: contractReports,
    };

    writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`Impact report written to ${outPath}`);
    console.log(`affectedContracts=${report.totals.affectedContracts}, failedTxCount=${report.totals.failedTxCount}`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
