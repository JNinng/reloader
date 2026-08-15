package reloader

// 日志属性键（对齐 observ attr 命名约定：snake_case，各库复用同名键）。
const (
	// AttrGeneration 资源世代号：标识资源年代，用于新旧识别与变更感知。
	AttrGeneration = "generation"

	// AttrReason 资源关闭原因枚举（drain / shutdown）。
	AttrReason = "reason"
)
