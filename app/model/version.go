package model

// UpgradeCheck 记录当前运行版本，返回上一次运行的版本以及是否发生了版本变化（首次运行不算升级）
func UpgradeCheck(current string) (previous string, upgraded bool) {
	previous = GetK(SystemVersion)
	if previous == current {
		return previous, false
	}

	SetK(SystemVersion, current)

	return previous, previous != ""
}
