# stress_performance

`stress_performance` 是一个用于测量 brpc TCP/RDMA 基础 RPC 性能的压测示例。
它支持 protobuf 请求、attachment 回显、闭环/开环负载、多连接以及延迟和 CPU
统计。该目录只使用 brpc 1.15.0 基线已有接口，可作为独立功能合入。

## 主要参数

Server：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--port` | `8002` | 服务监听端口 |
| `--transport` | `rdma` | `tcp` 或 `rdma`；显式设置时优先于 `--use_rdma` |
| `--use_rdma` | `true` | 兼容已有脚本；未显式设置 `--transport` 时决定是否使用 RDMA |
| `--server_num_threads` | `-1` | `-1` 保持默认值，`0` 由 bthread 并发度控制，正数显式设置 worker 数 |
| `--bthread_concurrency` | 运行库默认值 | bthread worker 并发度 |

Client：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--servers` | `0.0.0.0:8002+0.0.0.0:8002` | 以 `+` 分隔的服务地址 |
| `--transport` | `rdma` | `tcp` 或 `rdma` |
| `--thread_num` | `0` | 压测 worker 数；`0` 时使用 `max_thread_num` |
| `--queue_depth` | `1` | 闭环模式下每个 worker 的并发请求数 |
| `--attachment_size` | `-1` | attachment 字节数；负数表示不发送 |
| `--echo_attachment` | `false` | 服务端是否回显 attachment |
| `--connection_type` | `single` | Channel 连接类型 |
| `--connection_num` | `0` | Channel 数；`0` 时按 worker 数自动选择 |
| `--unique_connection_group` | `false` | 为每个 Channel 使用独立连接组 |
| `--load_mode` | `closed_loop` | `closed_loop` 或 `open_loop` |
| `--expected_qps` | `0` | 开环模式目标 QPS |
| `--max_inflight` | `0` | 开环模式全局在途请求上限 |
| `--rpc_timeout_ms` | `2000` | RPC 超时 |
| `--connect_timeout_ms` | `-1` | 建连超时；负数时使用 RPC 超时 |
| `--test_seconds` | `20` | 测试时长 |
| `--stop_grace_ms` | `5000` | 停止发压后等待在途请求的最长时间 |
| `--dummy_port` | `8001` | 客户端内置状态服务端口；`-1` 关闭 |
| `--record_latency` | `true` | 是否统计延迟分位数 |

## 构建

使用 Bazel：

```bash
bazel build -c opt //example:stress_performance_server \
  //example:stress_performance_client --define brpc_with_rdma=false
```

不具备 RDMA 环境时，应使用 `--transport=tcp`。使用 CMake 时，可在本目录建立
工作区外构建目录，并通过 `BRPC_OUTPUT_PATH` 指向已构建的 brpc `output`：

```bash
cmake -S example/stress_performance -B /tmp/brpc-stress-build \
  -DBRPC_OUTPUT_PATH="$PWD/output" -DWITH_RDMA=OFF
cmake --build /tmp/brpc-stress-build -j
```

## TCP 示例

```bash
./stress_performance_server \
  --transport=tcp \
  --port=8002 \
  --server_num_threads=32 \
  --bthread_concurrency=32
```

```bash
./stress_performance_client \
  --transport=tcp \
  --servers=127.0.0.1:8002 \
  --dummy_port=-1 \
  --thread_num=32 \
  --queue_depth=32 \
  --attachment_size=1024 \
  --echo_attachment=true \
  --connection_type=single \
  --bthread_concurrency=160 \
  --test_seconds=20
```

## RDMA 示例

RDMA 模式需要以 RDMA 支持构建 brpc，并确保设备、驱动和内存锁定限制配置正确：

```bash
./stress_performance_server --transport=rdma --port=8002
./stress_performance_client \
  --transport=rdma \
  --servers=127.0.0.1:8002 \
  --thread_num=32 \
  --queue_depth=32 \
  --test_seconds=20
```

## 结果说明

客户端输出平均延迟、P90/P99/P99.9 延迟、吞吐、QPS、完成数、失败数、超时数、
峰值在途请求以及客户端/服务端 CPU 使用率。对比不同传输方式时，应固定 CPU
亲和性、线程数、连接数、请求大小、测试时长和预热方式，并至少重复运行五次取
中位数。所有正式性能结果都应确认 `Failed` 和 `Timeout` 为零。

闭环模式大致维持 `thread_num * queue_depth` 个请求在途；开环模式按目标 QPS 发压，
更适合观察过载点和排队延迟。大 attachment 测试建议同时报告 QPS 和吞吐量，避免
只比较 QPS 而忽略实际传输字节数。
