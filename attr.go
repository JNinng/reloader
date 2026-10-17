package reloader

// 日志属性键：snake_case，各库复用同名键，保证用户侧跨库聚合口径一致；
// OTel 语义约定原名带点号的保留点号（error.type），带单位的属性统一
// _seconds 后缀。
const (
	// AttrGeneration 资源世代号：标识资源年代，用于新旧识别与变更感知。
	AttrGeneration = "generation"

	// AttrReason 资源关闭原因枚举（drain / shutdown）。
	AttrReason = "reason"

	// AttrErrorType 错误类型标识（错误动态类型或 panic 值的类型）。
	AttrErrorType = "error.type"

	// AttrDurationSeconds 耗时，单位秒（与 _seconds 指标口径一致）。
	AttrDurationSeconds = "duration_seconds"
)
