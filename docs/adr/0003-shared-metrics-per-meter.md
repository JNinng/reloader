# 指标按 Meter 去重共享

多个 Reloader 实例共用同一 Meter 时，指标对象按 Meter 只创建一次（首个实例创建，后续实例复用），counter/histogram 聚合为"本进程内库级活动"口径；`reloader_generation` 语义为最近一次成功换新的世代（跨实例）。理由：observ 规则禁 label、无法按实例拆分；prom 适配器同名重复 New* 为 MustRegister panic，若每实例独立注册，全应用共享一个 registry 的最自然用法会让第二个实例在构造期 panic。防御：Meter 动态类型不可比较时退化为每实例直建。

## Considered Options

- 每实例独立注册，文档要求多实例传独立 registry — 否决：构造期 panic 是显著 footgun。
