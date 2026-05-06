import { writeFileSync } from "fs";

const BEP652_ACTIVATION_TIME = 1777343400; // 2026-04-28 02:30:00 UTC on BSC mainnet.
const BEP652_ACTIVATION_BLOCK = 95_090_937;
const MAX_TX_GAS = 1 << 24; // EIP-7825 transaction gas limit cap: 16,777,216.
const DEFAULT_NEAR_CAP_THRESHOLD = 16_000_000;
const DEFAULT_FILTER_MODE = "strictCap";
const DEFAULT_WORKERS = 10;

function parseArgs(argv) {
    const args = {
        activationTime: BEP652_ACTIVATION_TIME,
        startNum: BEP652_ACTIVATION_BLOCK,
        gasThreshold: DEFAULT_NEAR_CAP_THRESHOLD,
        batchSize: 50,
        workers: DEFAULT_WORKERS,
        receiptConcurrency: 50,
        out: "bep652-first-pass-summary.json",
        txOut: "",
        filterMode: DEFAULT_FILTER_MODE,
        includeSuccess: false,
        skipRecoveryScan: false,
    };

    for (let i = 2; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === "-h" || arg === "--help") {
            args.help = true;
            continue;
        }
        if (arg === "--includeSuccess") {
            args.includeSuccess = true;
            continue;
        }
        if (arg === "--skipRecoveryScan") {
            args.skipRecoveryScan = true;
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

const args = parseArgs(process.argv);

function usage() {
    console.log("Usage:");
    console.log("  node scan_bep652_first_pass.js --rpc <rpc> [options]");
    console.log("");
    console.log("Examples:");
    console.log("  node scan_bep652_first_pass.js --rpc https://bsc-dataseed.bnbchain.org --endNum 49000000");
    console.log("  node scan_bep652_first_pass.js --rpc https://bsc-dataseed.bnbchain.org --startNum 95090937 --endNum 95100936 --out summary.json --txOut txs.json");
    console.log("");
    console.log("Notes:");
    console.log(`  Default start block: ${BEP652_ACTIVATION_BLOCK}.`);
    console.log(`  Default filterMode: ${DEFAULT_FILTER_MODE}.`);
    console.log("  strictCap: receipt.status = 0 AND tx.gas > 16,777,216 (above BEP-652 protocol cap).");
    console.log("  Summary also tracks likelyRevertCount (status=0 and gasUsed < gasLimit) and gasUsed > cap.");
    console.log("  nearCap: receipt.status = 0 AND receipt.gasUsed >= gasThreshold.");
    console.log(`  Parallelism: --workers (default ${DEFAULT_WORKERS}) controls how many batches run in parallel.`);
    console.log("  Recovery scan is enabled by default. Add --skipRecoveryScan to disable it.");
}

function requireOption(value, name) {
    if (!value) {
        throw new Error(`missing required option: ${name}`);
    }
}

function parseInteger(value, name) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
        throw new Error(`invalid ${name}: ${value}`);
    }
    return parsed;
}

function normalizeAddress(address) {
    if (!address) {
        return "contract_creation";
    }
    return address.toLowerCase();
}

function methodSelector(input) {
    if (!input || input === "0x" || input.length < 10) {
        return "0x";
    }
    return input.slice(0, 10);
}

function toNumber(value) {
    if (typeof value === "number") {
        return value;
    }
    if (typeof value === "bigint") {
        return Number(value);
    }
    if (typeof value === "string" && value.startsWith("0x")) {
        return Number.parseInt(value, 16);
    }
    return Number(value);
}

function defaultTxOut(summaryOut) {
    if (summaryOut.endsWith(".json")) {
        return summaryOut.slice(0, -5) + ".transactions.json";
    }
    return `${summaryOut}.transactions.json`;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, label, retries = 5) {
    let lastErr;
    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            return await fn();
        } catch (err) {
            lastErr = err;
            if (attempt === retries) {
                break;
            }
            const delay = 500 * 2 ** attempt;
            console.warn(`Retrying ${label} after error: ${err.message || err}. wait=${delay}ms`);
            await sleep(delay);
        }
    }
    throw lastErr;
}

class JsonRpcClient {
    constructor(url) {
        this.url = url;
        this.nextId = 1;
    }

    async call(method, params = []) {
        const res = await fetch(this.url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                jsonrpc: "2.0",
                id: this.nextId++,
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

    async getBlockNumber() {
        return toNumber(await this.call("eth_blockNumber"));
    }

    async getBlock(blockNumber, includeTransactions = false) {
        return await this.call("eth_getBlockByNumber", [`0x${blockNumber.toString(16)}`, includeTransactions]);
    }

    async getTransactionReceipt(txHash) {
        return await this.call("eth_getTransactionReceipt", [txHash]);
    }
}

async function mapWithConcurrency(items, limit, mapper) {
    const results = new Array(items.length);
    let nextIndex = 0;

    async function worker() {
        while (nextIndex < items.length) {
            const current = nextIndex++;
            results[current] = await mapper(items[current], current);
        }
    }

    const workers = [];
    const workerCount = Math.min(limit, items.length);
    for (let i = 0; i < workerCount; i++) {
        workers.push(worker());
    }
    await Promise.all(workers);
    return results;
}

function buildBatches(startBlock, endBlock, batchSize) {
    const batches = [];
    for (let s = startBlock; s <= endBlock; s += batchSize) {
        batches.push({ start: s, end: Math.min(s + batchSize - 1, endBlock) });
    }
    return batches;
}

async function runWorkerPool({ batches, workerCount, label, processBatch, getStatus }) {
    let nextIndex = 0;
    let completed = 0;

    async function worker() {
        while (true) {
            const idx = nextIndex++;
            if (idx >= batches.length) {
                return;
            }
            await processBatch(batches[idx]);
            completed++;
            if (completed === batches.length || completed % Math.max(1, Math.floor(batches.length / 50)) === 0) {
                const status = getStatus ? `, ${getStatus()}` : "";
                console.log(`${label} progress: ${completed}/${batches.length} batches${status}`);
            }
        }
    }

    const workers = [];
    const concurrent = Math.min(workerCount, batches.length);
    for (let i = 0; i < concurrent; i++) {
        workers.push(worker());
    }
    await Promise.all(workers);
}

async function findFirstBlockAtOrAfter(provider, timestamp, latestBlock) {
    let low = 0;
    let high = latestBlock;

    while (low < high) {
        const mid = Math.floor((low + high) / 2);
        const block = await withRetry(() => provider.getBlock(mid, false), `getBlock(${mid})`);
        if (!block) {
            throw new Error(`block ${mid} not found`);
        }
        if (toNumber(block.timestamp) >= timestamp) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

function classifyTx(tx) {
    if (tx.gasLimit > MAX_TX_GAS) {
        return tx.gasUsed === tx.gasLimit
            ? "status0_gas_limit_above_cap_oog_like"
            : "status0_gas_limit_above_cap_likely_revert";
    }
    if (tx.gasUsed === tx.gasLimit) {
        return "status0_oog_like_below_protocol_cap";
    }
    return "status0_likely_revert_other";
}

function shouldKeepFinding({ failed, gasLimit, gasUsed, gasThreshold, filterMode, includeSuccess }) {
    if (!includeSuccess && !failed) {
        return false;
    }
    if (filterMode === "strictCap") {
        return gasLimit > MAX_TX_GAS;
    }
    if (filterMode === "nearCap") {
        return gasUsed >= gasThreshold;
    }
    throw new Error(`unsupported --filterMode: ${filterMode}. expected strictCap or nearCap`);
}

function aggregateFindings(findings) {
    const aggregate = new Map();
    for (const tx of findings) {
        const key = `${tx.to}:${tx.selector}`;
        const entry = aggregate.get(key) || {
            category: tx.category,
            to: tx.to,
            selector: tx.selector,
            failedCount: 0,
            successCount: 0,
            maxGasUsed: 0,
            maxGasLimit: 0,
            status0Count: 0,
            likelyRevertCount: 0,
            oogLikeCount: 0,
            gasLimitAboveCapCount: 0,
            gasUsedAboveCapCount: 0,
            firstBlock: tx.blockNumber,
            lastBlock: tx.blockNumber,
            callers: {},
            sampleTxs: [],
        };

        if (tx.status === 0) {
            entry.failedCount++;
            entry.status0Count++;
            if (tx.gasUsed < tx.gasLimit) {
                entry.likelyRevertCount++;
            } else if (tx.gasUsed === tx.gasLimit) {
                entry.oogLikeCount++;
            }
        } else {
            entry.successCount++;
        }
        entry.maxGasUsed = Math.max(entry.maxGasUsed, tx.gasUsed);
        entry.maxGasLimit = Math.max(entry.maxGasLimit, tx.gasLimit);
        if (tx.gasLimit > MAX_TX_GAS) {
            entry.gasLimitAboveCapCount++;
        }
        if (tx.gasUsed > MAX_TX_GAS) {
            entry.gasUsedAboveCapCount++;
        }
        entry.firstBlock = Math.min(entry.firstBlock, tx.blockNumber);
        entry.lastBlock = Math.max(entry.lastBlock, tx.blockNumber);
        entry.callers[tx.from] = (entry.callers[tx.from] || 0) + 1;
        if (entry.sampleTxs.length < 5) {
            entry.sampleTxs.push(tx.hash);
        }

        aggregate.set(key, entry);
    }

    return Array.from(aggregate.values()).sort((a, b) => {
        if (b.failedCount !== a.failedCount) {
            return b.failedCount - a.failedCount;
        }
        return b.maxGasUsed - a.maxGasUsed;
    });
}

function emptyRecovery() {
    return {
        sameSelectorSuccessAfterFailure: {
            count: 0,
            first: null,
            latest: null,
            sampleTxs: [],
        },
        sameContractSuccessAfterFailure: {
            count: 0,
            first: null,
            latest: null,
            selectors: {},
            sampleTxs: [],
        },
    };
}

function updateRecoveryBucket(bucket, tx) {
    bucket.count++;
    if (!bucket.first || tx.blockNumber < bucket.first.blockNumber) {
        bucket.first = tx;
    }
    if (!bucket.latest || tx.blockNumber > bucket.latest.blockNumber) {
        bucket.latest = tx;
    }
    if (bucket.selectors) {
        bucket.selectors[tx.selector] = (bucket.selectors[tx.selector] || 0) + 1;
    }
    if (bucket.sampleTxs.length < 5) {
        bucket.sampleTxs.push(tx.hash);
    }
}

async function scanRecoveries({
    provider,
    endBlock,
    groups,
    batchSize,
    workerCount,
    receiptConcurrency,
}) {
    if (groups.length === 0) {
        return {
            recoveriesByGroup: {},
            recoveryTransactionsByCategory: {},
        };
    }

    const groupByKey = new Map(groups.map((group) => [`${group.to}:${group.selector}`, group]));
    const affectedContracts = new Set(groups.map((group) => group.to));
    const recoveriesByGroup = Object.fromEntries(
        groups.map((group) => [`${group.to}:${group.selector}`, emptyRecovery()])
    );
    const recoveryTransactionsByCategory = {
        recovered_success_same_selector: [],
        contract_success_other_selector: [],
    };
    const recoveryStartBlock = Math.min(...groups.map((group) => group.firstBlock));

    console.log(
        `Recovery scan: range=${recoveryStartBlock}-${endBlock}, contracts=${affectedContracts.size}, workers=${workerCount}`
    );

    const batches = buildBatches(recoveryStartBlock, endBlock, batchSize);

    async function processBatch(batch) {
        const blocks = await Promise.all(
            Array.from({ length: batch.end - batch.start + 1 }, (_, index) => {
                const blockNum = batch.start + index;
                return withRetry(
                    () => provider.getBlock(blockNum, true),
                    `recovery getBlock(${blockNum}, true)`
                );
            })
        );

        const candidates = [];
        for (const block of blocks) {
            if (!block) {
                continue;
            }
            for (const tx of block.transactions) {
                const to = normalizeAddress(tx.to);
                if (!affectedContracts.has(to)) {
                    continue;
                }
                candidates.push({
                    block,
                    tx,
                    to,
                    selector: methodSelector(tx.input || "0x"),
                    gasLimit: toNumber(tx.gas),
                });
            }
        }

        const receipts = await mapWithConcurrency(candidates, receiptConcurrency, ({ tx }) => {
            return withRetry(
                () => provider.getTransactionReceipt(tx.hash),
                `recovery getTransactionReceipt(${tx.hash})`
            );
        });

        for (let i = 0; i < candidates.length; i++) {
            const candidate = candidates[i];
            const receipt = receipts[i];
            if (!receipt || Number(receipt.status) !== 1) {
                continue;
            }

            const txInfo = {
                blockNumber: toNumber(candidate.block.number),
                blockTime: toNumber(candidate.block.timestamp),
                hash: candidate.tx.hash,
                from: normalizeAddress(candidate.tx.from),
                to: candidate.to,
                selector: candidate.selector,
                status: 1,
                gasLimit: candidate.gasLimit,
                gasUsed: toNumber(receipt.gasUsed),
                gasPrice: candidate.tx.gasPrice ? toNumber(candidate.tx.gasPrice).toString() : null,
            };

            for (const group of groups) {
                if (group.to !== txInfo.to || txInfo.blockNumber <= group.firstBlock) {
                    continue;
                }

                const groupKey = `${group.to}:${group.selector}`;
                updateRecoveryBucket(
                    recoveriesByGroup[groupKey].sameContractSuccessAfterFailure,
                    txInfo
                );

                if (groupByKey.has(`${txInfo.to}:${txInfo.selector}`)) {
                    updateRecoveryBucket(
                        recoveriesByGroup[groupKey].sameSelectorSuccessAfterFailure,
                        txInfo
                    );
                    recoveryTransactionsByCategory.recovered_success_same_selector.push(txInfo);
                } else {
                    recoveryTransactionsByCategory.contract_success_other_selector.push(txInfo);
                }
            }
        }
    }

    await runWorkerPool({ batches, workerCount, label: "Recovery", processBatch });

    return {
        recoveriesByGroup,
        recoveryTransactionsByCategory,
    };
}

async function scan() {
    if (process.argv.length <= 2 || args.help) {
        usage();
        return;
    }

    requireOption(args.rpc, "--rpc");
    const provider = new JsonRpcClient(args.rpc);
    const latestBlock = await withRetry(() => provider.getBlockNumber(), "getBlockNumber");
    const activationTime = parseInteger(args.activationTime, "--activationTime");
    const gasThreshold = parseInteger(args.gasThreshold, "--gasThreshold");
    const batchSize = parseInteger(args.batchSize, "--batchSize");
    const workerCount = parseInteger(args.workers, "--workers");
    const receiptConcurrency = parseInteger(args.receiptConcurrency, "--receiptConcurrency");
    const filterMode = args.filterMode;

    if (gasThreshold > MAX_TX_GAS) {
        throw new Error(`--gasThreshold cannot be greater than BEP-652 cap ${MAX_TX_GAS}`);
    }
    if (workerCount < 1) {
        throw new Error(`--workers must be >= 1`);
    }

    const startBlock =
        args.startNum === "auto"
            ? await findFirstBlockAtOrAfter(provider, activationTime, latestBlock)
            : parseInteger(args.startNum, "--startNum");
    const endBlock = args.endNum ? parseInteger(args.endNum, "--endNum") : latestBlock;

    if (startBlock > endBlock) {
        throw new Error(`start block ${startBlock} is greater than end block ${endBlock}`);
    }

    if (!["strictCap", "nearCap"].includes(filterMode)) {
        throw new Error(`unsupported --filterMode: ${filterMode}. expected strictCap or nearCap`);
    }

    const txOut = args.txOut || defaultTxOut(args.out);

    console.log(
        `BEP-652 first-pass scan: range=${startBlock}-${endBlock}, filterMode=${filterMode}, workers=${workerCount}, batchSize=${batchSize}`
    );
    console.log(`summary=${args.out}`);
    console.log(`transactions=${txOut}`);

    const findings = [];
    const startedAt = Date.now();
    const batches = buildBatches(startBlock, endBlock, batchSize);

    async function processBatch(batch) {
        const blocks = await Promise.all(
            Array.from({ length: batch.end - batch.start + 1 }, (_, index) => {
                const blockNum = batch.start + index;
                return withRetry(
                    () => provider.getBlock(blockNum, true),
                    `getBlock(${blockNum}, true)`
                );
            })
        );

        const candidates = [];
        for (const block of blocks) {
            if (!block) {
                continue;
            }
            for (const tx of block.transactions) {
                if (!tx) {
                    continue;
                }
                const gasLimit = toNumber(tx.gas);
                const meetsCandidate =
                    filterMode === "strictCap" ? gasLimit > MAX_TX_GAS : gasLimit >= gasThreshold;
                if (meetsCandidate) {
                    candidates.push({ block, tx, gasLimit });
                }
            }
        }

        if (candidates.length === 0) {
            return;
        }

        const receipts = await mapWithConcurrency(candidates, receiptConcurrency, ({ tx }) => {
            return withRetry(
                () => provider.getTransactionReceipt(tx.hash),
                `getTransactionReceipt(${tx.hash})`
            );
        });

        for (let i = 0; i < candidates.length; i++) {
            const { block, tx, gasLimit } = candidates[i];
            const receipt = receipts[i];
            if (!receipt) {
                continue;
            }

            const gasUsed = toNumber(receipt.gasUsed);
            const status = receipt.status === null ? null : Number(receipt.status);
            const failed = status === 0;
            if (
                !shouldKeepFinding({
                    failed,
                    gasLimit,
                    gasUsed,
                    gasThreshold,
                    filterMode,
                    includeSuccess: args.includeSuccess,
                })
            ) {
                continue;
            }

            const finding = {
                category: classifyTx({ gasLimit, gasUsed }),
                blockNumber: toNumber(block.number),
                blockTime: toNumber(block.timestamp),
                hash: tx.hash,
                from: normalizeAddress(tx.from),
                to: normalizeAddress(tx.to),
                selector: methodSelector(tx.input || "0x"),
                status,
                gasLimit,
                gasUsed,
                gasPrice: tx.gasPrice ? toNumber(tx.gasPrice).toString() : null,
            };
            findings.push(finding);
            console.log(
                `MATCH block=${finding.blockNumber} status=${finding.status} gasLimit=${finding.gasLimit} gasUsed=${finding.gasUsed} to=${finding.to} selector=${finding.selector} tx=${finding.hash}`
            );
        }
    }

    await runWorkerPool({
        batches,
        workerCount,
        label: "Main",
        processBatch,
        getStatus: () => `findings=${findings.length}`,
    });

    findings.sort((a, b) => a.blockNumber - b.blockNumber);

    const groups = aggregateFindings(findings);
    const recoveryResult = args.skipRecoveryScan
        ? { recoveriesByGroup: {}, recoveryTransactionsByCategory: {} }
        : await scanRecoveries({
              provider,
              endBlock,
              groups,
              batchSize,
              workerCount,
              receiptConcurrency,
          });

    for (const group of groups) {
        const recovery = recoveryResult.recoveriesByGroup[`${group.to}:${group.selector}`];
        group.recovery = recovery || emptyRecovery();
    }

    const summary = {
        meta: {
            rpc: args.rpc,
            startBlock,
            endBlock,
            activationTime,
            gasThreshold,
            filterMode,
            maxTxGas: MAX_TX_GAS,
            workers: workerCount,
            batchSize,
            includeSuccess: Boolean(args.includeSuccess),
            recoveryScan: !args.skipRecoveryScan,
            generatedAt: new Date().toISOString(),
            elapsedSeconds: (Date.now() - startedAt) / 1000,
        },
        totalFindings: findings.length,
        totalLikelyReverts: findings.filter((tx) => tx.status === 0 && tx.gasUsed < tx.gasLimit).length,
        totalOogLike: findings.filter((tx) => tx.status === 0 && tx.gasUsed === tx.gasLimit).length,
        totalGasUsedAboveCap: findings.filter((tx) => tx.status === 0 && tx.gasUsed > MAX_TX_GAS).length,
        groups,
    };

    const transactionsByCategory = findings.reduce((acc, tx) => {
        if (!acc[tx.category]) {
            acc[tx.category] = [];
        }
        acc[tx.category].push(tx);
        return acc;
    }, {});

    const details = {
        meta: summary.meta,
        transactionsByCategory,
        recoveryTransactionsByCategory: recoveryResult.recoveryTransactionsByCategory,
    };

    writeFileSync(args.out, `${JSON.stringify(summary, null, 2)}\n`);
    writeFileSync(txOut, `${JSON.stringify(details, null, 2)}\n`);

    console.log(`Done: totalFindings=${summary.totalFindings}, groups=${summary.groups.length}`);
    for (const group of summary.groups) {
        console.log(
            `${group.category}: status0=${group.status0Count}, likelyRevert=${group.likelyRevertCount}, oogLike=${group.oogLikeCount}, gasUsedAboveCap=${group.gasUsedAboveCapCount}, to=${group.to}, selector=${group.selector}, maxGasUsed=${group.maxGasUsed}, sameSelectorRecovered=${group.recovery.sameSelectorSuccessAfterFailure.count}, sameContractSuccess=${group.recovery.sameContractSuccessAfterFailure.count}`
        );
    }
}

scan().catch((err) => {
    console.error(err);
    process.exit(1);
});
