# 快速入门

本文档提供了bRPC新增memfd、消息自适应批处理特性使能的性能描述和配置步骤，旨在为用户提供bRPC快速入门指导。

## 环境要求

| 项目 | 说明 |
| --- | --- |
| 硬件 | 鲲鹏950处理器 |
| 操作系统 | openEuler 24.03 |
| 编译器 | g++ v12版本 |

## 使能优化

基于bRPC框架，鲲鹏bRPC可进一步提升bRPC框架通信和消息处理能力。在以下方面进行了优化：

- 支持MEMFD共享内存通信；
- 支持输入消息批处理。

### 源码安装bRPC

1. 获取bRPC源码。

   ```bash
   git clone https://github.com/apache/brpc.git
   cd brpc
   git checkout 50a9075de62f1d2b825b902acbfecfddb3d9f314
   ```

2. 获取并合入补丁。
   ```bash
   wget https://raw.gitcode.com/boostkit/brpc/raw/main/brpc_memfd_batch.patch
   git apply brpc_memfd_batch.patch
   ```

   使能后目录结构：

      ```text
      brpc/
      ├── .github/                 # GitHub工作流及项目配置
      ├── bazel/                   # Bazel构建支持
      ├── cmake/                   # CMake构建支持
      ├── community/               # 社区案例及相关资料
      ├── docs/                    # 项目文档
      │   ├── zh/                  # 中文文档
      │   ├── en/                  # 英文文档
      │   └── images/              # 文档图片资源
      ├── example/                 # 功能示例及性能测试程序
      ├── homebrew-formula/        # Homebrew安装支持
      ├── java/                    # Java扩展预留目录
      ├── package/                 # 软件包构建配置
      ├── python/                  # Python扩展预留目录
      ├── src/                     # 核心源代码
      │   ├── brpc/                # RPC框架、协议及传输实现
      │   │   ├── builtin/         # 内置服务
      │   │   ├── details/         # 框架内部实现
      │   │   ├── memfd/           # MEMFD共享内存通信
      │   │   ├── policy/          # RPC协议与策略实现
      │   │   └── rdma/            # RDMA通信实现
      │   ├── bthread/             # 用户态线程库
      │   ├── butil/               # 基础工具库
      │   ├── bvar/                # 指标统计组件
      │   ├── json2pb/             # JSON与Protobuf转换组件
      │   └── mcpack2pb/           # Mcpack与Protobuf转换组件
      ├── test/                    # 单元测试与Fuzz测试
      │   └── fuzzing/             # Fuzz测试及种子语料
      └── tools/                   # 构建、诊断及辅助工具
      ```


## 使用示例（使用benchmark stress_performance进行性能测试）

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
4. 性能对比。

   在相同软硬件环境和压测参数下，对比开源版本原有通信处理路径与启用MEMFD共享内存通信、输入消息自适应批处理后的优化版本。QPS取5次测试的平均值，单位为k。QPS提升比的计算公式如下：

   ```text
   QPS提升比 = (优化版本QPS Avg - 开源版本QPS Avg) / 开源版本QPS Avg × 100%
   ```

   测试结果如下：

   | attachment_size | 开源版本QPS Avg（k） | 优化版本QPS Avg（k） | QPS提升比（%） |
   | --- | ---: | ---: | ---: |
   | 0 bytes | 2684.100 | 3348.759 | 24.76 |
   | 1K | 2174.254 | 2418.618 | 11.24 |
   | 4K | 1486.802 | 1601.845 | 7.74 |
   | 8K | 1049.357 | 1207.340 | 15.06 |
   | 100K | 134.602 | 254.740 | 89.25 |
   | 200K | 68.524 | 137.548 | 100.73 |
   | 1M | 13.791 | 27.696 | 100.82 |
   | 8M | 1.582 | 4.010 | 153.48 |


   测试结果表明，优化版本在各消息大小下均有提升。其中，100K及以上大消息场景的QPS提升为89.25%～153.48%。
