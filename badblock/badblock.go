package badblock

var BadBlock = 0

func InitBadBlock() {
	BadBlock = 0
}

func SetBadBlock() {
	BadBlock = 1
}

func HasBadBlock() bool {
	if BadBlock == 1 {
		return true
	}

	return false
}
