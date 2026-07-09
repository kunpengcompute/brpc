# 简介
ub_performance_press是bRPC性能测试工具，用于测试bRPC over UBSocket场景的性能。

本工具是 `ub_performance` 的"压测增强版"：采用 v5 proto（proto3 `Request`/`ValueMap`/`Value` + oneof）与随机数据生成方式，并预生成 Request 池，更贴近真实业务负载形态下的吞吐/时延压测。

> 说明：
> bRPC代码仓中的rdma_performance工具，已经较长时间（10个月）未修改。bRPC_v1.12.1至bRPC_v1.15.0版本的rdma_performance工具完全一致。

# 与 ub_performance 的差异

| 维度 | ub_performance | ub_performance_press |
| --- | --- | --- |
| proto 版本 | proto2 | proto3 |
| 消息结构 | `PerfTestRequest{echo_attachment, name}` / `PerfTestResponse{cpu_usage, name}` | `Request{common_feat, repeated ad_feat, labels}` + `Value`(oneof 8 类型) + `ValueMap`(map) |
| 负载生成 | `--req_size` 用固定字符 `'r'` 填充 `name` 字段；服务端 `--rsp_size` 填充响应 `name` | `data_generator.h` 按对数均匀分布(1K~1M)随机生成，启动时预生成 Request 池，每次 RPC round-robin 引用，`ByteSizeLong` 预计算缓存 |
| 关键 flags | `--req_size`、`--echo_attachment`（客户端经 proto 传递给服务端）、`--rsp_size` | `--num_datasets`、`--seed`、`--min_size`、`--max_size`、`--fixed_size`；服务端 `--echo_attachment` 直接读取，不再经 proto 传递 |
| 服务端实现 | 构造响应、回填 `cpu_usage`/`name` | `response->Swap(const_cast<Request*>(request))`（O(1) 指针交换）；启用 Arena 消息工厂 |
| 吞吐统计口径 | `req_size!=0` 取 `resp->name().size()`，否则取 attachment 大小 | `request_size`(预计算) + `request_attachment().size()` |
| 性能打点 | 已合入 `BRPC_ENABLE_TRACE_SCOPE` 打点 | 已合入 `BRPC_ENABLE_TRACE_SCOPE` 打点（与 ub_performance 一致） |

> 两者均支持 `BRPC_ENABLE_TRACE_SCOPE` 性能打点（client/server 启动时分配 step latency 存储，结束时输出分步时延统计），代码一致。

# 使用方式
下载bRPC源码，参考[bazel编译bRPC](https://gitcode.com/fanzhaonan/brpc/blob/develop/docs/cn/bazel_brpc.md)及[brpc支持rdma](https://github.com/apache/brpc/blob/release-1.15/docs/cn/rdma.md)的介绍进行编译和运行即可。

建议编译命令：

`bazel build -c opt //example:ub_performance_press_server --define brpc_with_urma=true`

`bazel build -c opt //example:ub_performance_press_client --define brpc_with_urma=true`

> 说明：
> 不同的测试用例，在执行client、server时需要指定不同的参数，可在参考文档中了解。
