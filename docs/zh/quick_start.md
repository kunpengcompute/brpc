# 快速入门

本文档提供了bRPC新增特性使能的性能描述和配置步骤，旨在为用户提供bRPC快速入门指导。

## 环境要求

- 已验证的硬件配置：鲲鹏950处理器

- 已验证的OS：openEuler 24.03

- 已验证的编译器：g++-12

## 使能优化

基于bRPC框架，鲲鹏bRPC可进一步提升bRPC框架通信和消息处理能力。在以下方面进行了优化：

- 支持MEMFD共享内存通信；
- 支持输入消息批处理。

### 配置bRPC

   获取bRPC源码。

   ```bash
   git clone https://gitcode.com/boostkit/brpc.git
   cd brpc
   git checkout dev_brpc_930
   ```

优化代码已和入分支`dev_brpc_930`

## 使用示例（使用新增benchmark stress_performance 进行性能测试）

1. 编译benchmark stress_performance。

   ```bash
   bazel build -c opt //example:stress_performance_client //example:stress_performance_server --define BRPC_WITH_RDMA==false
   ```

2. 运行benchmark/server。

   ```bash
   ./bazel-bin/example/stress_performance_server \
    --use_rdma=false \
    --port=10086 \
    --transport=memfd \
    --input_message_batch_process_size=-1
   ```

3. 运行benchmark/client。

   ```bash
   ./bazel-bin/example/stress_performance_client \
    --use_rdma=false \
    --transport=memfd \
    --input_message_batch_process_size=-1
    --protocol=baidu_std    \
    --connection_type=single     \
    --servers=127.0.0.1:10086      \
    --thread_num=1         \
    --queue_depth=1         \
    --attachment_size=1024      \
    --echo_attachment=true       \
    --load_mode=closed_loop        \
    --connection_num=1       \
    --unique_connection_group=false     \
    --test_seconds=10
   ```

    回显如下结果说明使能成功。
  
   ```test
   [Threads: 1, Depth: 1, Attachment: 1024B, Transport: memfd, Echo: yes, RecordLatency: true, LoadMode: closed_loop, ConnectionType: single, ConnectionNum: 1, RpcTimeoutMs: 10000, ConnectTimeoutMs: 10000, StopGraceMs: 5000, UniqueConnectionGroup: true, ModelConcurrency: 1024]
   Closed-loop keeps roughly thread_num * queue_depth requests in flight.
   Avg-Latency: 439, 90th-Latency: 669, 99th-Latency: 995, 99.9th-Latency: 1323, Throughput: 2259.4MB/s, QPS: 2313.622k, Completed: 23142696, Failed: 0, Timeout: 0, PeakInflight: 1024, Server CPU-utilization: 622%, Client CPU-utilization: 3335%
   ```
