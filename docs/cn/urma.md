# URMA 共享队列传输

URMA 通过统一连接资源框架支持两种后端：`--urma_resource_mode=exclusive`（默认）保留每连接独占 Jetty 的实现；`--urma_resource_mode=pooled` 使用共享 JFS/JFR。下文池化架构、配置和示例均指显式选择 `pooled` 的情况。两种后端使用各自的资源描述；两端模式一致时建立 URMA，模式不匹配时协商回退到 TCP。

UrmaTransport 使用 openEuler [UMDK](https://atomgit.com/openeuler/umdk) 的
URMA 接口承载 brpc 消息。当前数据通路采用可靠消息模式 `URMA_TM_RM`、
CTP 和独立的 JFS/JFR，通过共享硬件队列服务多个逻辑连接。

## 资源组织

`UrmaMemoryPool` 管理已注册内存，`UrmaPooledResource` 管理队列及其生命周期，
`UrmaPooledConnection` 保存连接状态并负责共享 CQ 处理、接收路由、重排与有序交付；
`UrmaExclusiveConnection` 封装社区独占实现。两者通过 `UrmaConnection` 共用连接接口，
由 `UrmaConnectionProvider` 管理创建与运行环境。`UrmaEndpoint` 保留握手、输入缓存和消息解析，
池化 Connection 内部的 `UrmaRxDispatchContext` 管理一次 CQ 处理中的解析批次和最后消息交付边界。

JFS 是发送队列，JFR 是接收队列。每个 slot 独立持有 JFC、JFCE 和用于
事件分发的 Socket。进程默认创建 48 个 JFS、64 个 JFR，队列数量不随
逻辑连接数量线性增长。每个连接保存分配的本地 JFR、对端资源引用、
SocketId、收发序号以及重排状态，不单独创建一套硬件队列。

对端 JFR 导入缓存使用 EID、UASID 和 JFR ID 共同作为键，以引用计数
共享导入结果。连接销毁后，未完成发送仍持有相关资源引用，直到完成处理
释放它们。

## 注册内存池

默认内存池使用 mmap 分配连续内存并注册为 URMA segment，以固定大小的
块进行分配和回收。数据池的默认块大小为 8KiB，供 IOBuf 使用；可用数据
空间需扣除 IOBuf 的块头。CTP 池提供独立的 4KiB 块，用于接收
body；扣除块内 IOBuf 元数据后仍可容纳完整的 4032B body。独立 header
池的块大小为 64B，不经过 IOBuf 块分配器。发送直接引用已注册 IOBuf，
包括 8KiB 数据块和收到的 4KiB CTP 块，不为切换内存池而复制 body。

空闲块分为 64 个分片，每个分片分别加锁。分配从当前线程对应的分片开始，
必要时扫描其他分片。内存池初始化后不自动扩容；耗尽时返回失败。
注销注册内存前必须先结束 DMA。仍被应用 IOBuf 引用的数据块需要保留
其映射及回收器；CTP 池在所有应用引用归还前同时保留注册信息。

`UrmaMemoryPool` 提供分配、回收、地址归属、segment 查询和注销接口。
可在全局初始化前通过 `SetUrmaMemoryPoolFactory` 更换实现，由全局初始化
创建并安装数据池、CTP 池与 header 池。替代实现必须保持内存注册及引用生命周期
约束。用户注册数据通过 `RegisterMemoryForUrma` 和
`DeregisterMemoryForUrma` 管理；发送期间不得注销仍被使用的内存。

## 建链与协商

连接先通过 TCP 完成握手，再切换到 URMA 数据通路。握手交换接收块大小、
接收队列信息、带版本的 SocketId、EID/UASID、传输路径类型和 segment
描述。客户端支持二进制 v2 和 Protobuf v3 两种握手编码，默认使用 v2。
共享队列在 v2 使用 implementation=2，在 v3 使用 resource_mode=2；
独占模式对应值为1。双方校验远端资源及地址范围。

协商与资源导入完成后才发布 `URMA_ON` 状态。模式不匹配时完整消费 Hello，
通过拒绝响应或 ACK=0 协商 TCP 回退，避免握手残留字节进入业务解析。
I/O 错误及非法资源描述仍按相应错误路径处理。并发连接各自持有握手对象，
避免复用仍在进行中的回调状态。

## 发送路径

每个 SEND 分片带有 64B header，包含目标 SocketId、16位序号和消息类型，
多字节字段使用网络字节序。CTP 的单次消息总长度不超过 4096B，因此
header 之后最多承载 4032B body；实际长度还受双方接收能力约束。

多 SGE 发送使用一个 header SGE，加上一个或多个 body SGE。
发送函数跨输入 IOBuf 及其数据块收集 body，直到达到总 SGE 上限或 body
长度上限。已注册 body 直接引用原内存，不为拼接报文复制整个 payload。
未注册的片段不能直接作为 DMA 源。

接收采用 header/body 两个 SGE。

默认按 SocketId 哈希确定首选 JFS。首选容量不足时，从最多4个不重复备选中
选择在途请求最少者，一次最多尝试首选和一个备选。关闭 `urma_jfs_affinity`
时按批轮询首选。每个 JFS 的 MPSC 请求队列由单一执行者推进，单次最多
链式提交 `urma_io_tx_batch_max` 个 WR；仅收集已发布请求，不等待凑满。

请求发布到资源队列即视为接受，调用方移除对应的连续字节前缀。资源层持有
header、SGE、数据和导入资源，直到提交及 CQE 引用均释放。暂未成功 post
的后缀保留在队列中，由完成事件继续推进；若容量已在 post 返回前归还，
直接重试后缀。共享 SQ 的等待者登记在首选 JFS，按容量通知，退出时全部唤醒。

发送完成返回 ACK 超时时，发送执行者暂停向该 JFS 提交新 WR。全部已提交 WR 收到完成通知后，执行者在原槽位和 JFC 上重建 JFS，再继续提交其他连接排队的 WR。已提交的 WR 不会重发。若重建失败，该槽位停止服务，排队连接会收到错误。

## 接收、重排与交付

每个 JFR 预投递多个接收上下文。接收完成后检查 header 和数据长度，
接收适配器使用目标 SocketId 查找逻辑连接，在池化连接资源中按分片序号
恢复有序字节流，并在交付锁内再次核对资源身份后进入 Endpoint 输入缓存。
顺序到达的分片走快速路径，不插入重排 map；乱序分片暂存，缺口补齐后
连续交付。序号处理支持16位回绕，重复或非法分片会使连接失败。

接收上下文及 header 可在补收时复用，已交给应用的 body 由 IOBuf 引用
保活。IOBuf 交接使用 movable 转移引用，避免按值浅拷贝带来的额外引用
维护；这不等同于复制 payload。

默认按一次 CQ poll 的结果收集需要解析的连接，完成该批次的接收处理后，
每个连接调用一次消息解析入口。此批次按连接合并解析调用，不改变消息
协议边界或同一连接内的字节顺序。

完成事件处理支持延后执行最后一条消息：先完成 CQ 事件交接并释放相关锁，
再允许整个交付范围内至多一条消息在当前 bthread 执行。其余消息通过
消息调度机制处理，避免业务回调持有共享队列锁。

## 并发与退出

JFS 的发送执行者串行提交 WR，JFR/JFC 使用各自的接收和完成执行者。
操作屏障协调 post、poll 与销毁；TX 使用原子登记数组，RX 使用固定上下文
数组和 posted 标记。Socket 和资源池共同持有 slot 引用，防止排队的事件
回调访问已释放对象。逻辑连接仍使用重排锁和交付锁维护字节流顺序。

关闭时先停止事件与队列活动，唤醒发送等待者，并按资源依赖释放本地队列、
完成上下文和导入资源。内存注销以 DMA 结束为前提，存储释放还需保证应用引用不再使用它；不能
为了回收对象而提前释放设备或应用仍可能访问的内存。

## 构建与启用

在真实设备上启用该模块需要 UMDK 头文件和 `liburma`。CMake 使用
`WITH_URMA=ON`、`URMA_USE_MOCK=OFF`，
优先查找安装的 SDK；找不到头文件时可下载固定版本的 UMDK 头文件，
`DOWNLOAD_URMA_HEADERS=OFF` 可禁止下载。可通过 `URMA_ROOT`、
`URMA_INCLUDE_PATH`、`URMA_BOND_INCLUDE_PATH` 和 `URMA_LIB` 指定 SDK。

Make 使用 `config_brpc.sh` 的 `--with-urma --without-urma-mock`，
并通过 `--headers`、`--libs` 提供依赖路径。Bazel 使用 `BRPC_WITH_URMA=true`、
`BRPC_URMA_USE_MOCK=false`，链接真实的 `liburma`；
关闭 `BRPC_DOWNLOAD_URMA_HEADERS` 后，需要由工具链或本地依赖声明头文件。

客户端和服务端分别通过 `ChannelOptions::use_urma` 与
`ServerOptions::use_urma` 启用传输。当前支持 `baidu_std` 协议，使用
非 bonding 设备及事件驱动模式；不支持忙轮询、端到端 credit/ACK 流控，
池化模式新增可选的大包 READ，默认仍分片 SEND；不包含单边 WRITE。READ要求两端协商能力和注册内存段，不支持READ的对端继续使用SEND。

## 主要运行参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `urma_device` | 空 | 指定设备；空值选择枚举到的首个设备 |
| `urma_eid_index` | -1 | 启动时选择 EID；-1 使用 EID 列表第一项，非负值匹配设备返回的实际 `eid_index`；不存在或小于 -1 时初始化失败 |
| `urma_io_jfs_cnt` | 48 | 进程共享发送队列数量 |
| `urma_io_jfr_cnt` | 8 | 进程共享接收队列数量 |
| `urma_io_sq_depth` | 128 | 每个发送队列的在途槽位数 |
| `urma_io_jfs_err_timeout` | 17 | 池化 JFS 错误超时指数（0–31）。调低可更快检测 ACK 超时，调高可容忍更长的 ACK 延迟。 |
| `urma_io_rx_depth` | 256 | 每个接收队列的预投递深度 |
| `urma_jfs_affinity` | true | 按 SocketId 哈希选择固定首选 JFS |
| `urma_io_tx_batch_max` | 8 | 每批准备与链式提交的 WR 上限，范围1至32 |
| `urma_io_jfs_lock_free` | false | 设置 UMDK JFS 单执行者属性 |
| `urma_io_jfr_lock_free` | false | 设置 UMDK JFR 单执行者属性 |
| `urma_io_jfc_lock_free` | false | 设置 UMDK JFC 单执行者属性 |
| `urma_buffer_size` | 8192 | 数据池块大小，包含 IOBuf 块头 |
| `urma_buffer_count` | 池化5120；独占65536 | 未显式设置时按模式选择；显式值覆盖池化默认 |
| `urma_ctp_buf_count` | 10240 | 4KiB CTP 块数量；必须大于接收预投递总数 |
| `urma_max_sge` | 0 | 0采用设备能力；正值限制发送 SGE 数 |
| `urma_cqe_poll_once` | 32 | 每次 poll 最多取出的 CQE 数 |
| `urma_rx_batch_parse` | true | 同一 poll 中按连接合并解析调用 |
| `urma_client_handshake_version` | 2 | 客户端握手编码，支持2或3 |

队列数量、深度与内存池参数应在全局初始化前确定。接收预投递需要充足
CTP 块，header 数量需要覆盖接收和发送在途槽位；初始化会校验组合是否
可用。硬件能力同样约束队列深度、SGE 数和消息大小。

## 池化大包 READ

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `urma_large_transfer` | send | send/read/auto；auto在协商成功且消息超过单次SEND载荷上限时采用READ |
| `urma_large_block_size` | 65536 | 注册大IOBuf块大小，包含IOBuf块头 |
| `urma_large_block_threshold` | 4096 | 无压缩protobuf body超过该字节数时直接序列化到大块 |
| `urma_large_buffer_count` | 2048 | 大块池数量；0禁用READ能力 |
| `urma_read_items_per_control` | 32 | 每个PULL控制消息携带的描述符上限，范围1至99 |
| `urma_memory_pool_limit_mb` | 512 | 池化注册内存总上限，包含新增大块池 |

两端指定 `--urma_resource_mode=pooled --urma_large_transfer=read` 即可启用。PULL通过双边SEND发布源地址，接收方READ完成后提交FREE，随后通过现有有序字节流回调交付。FREE确认前源IOBuf一直保留，不能按控制SEND的TX CQE释放。每个源片段使用独立seq，多个IOBuf可共享一个控制消息。

READ CQE由JFS执行者处理，SEND接收由JFR执行者处理；同一连接的两条交付路径继续使用现有重排/解析锁。READ完成时整批重排，再按CQ轮次批量解析，最后消息遵守现有回调退出后的交付约束。硬件资源池仅同步传递不透明的本轮完成调度参数，不持有池化连接的私有调度类型。

本阶段未增加超时重传或失联源缓冲回收协议；断链后尚未收到FREE的源缓冲可能保留，以避免对端仍在READ时提前释放内存。非池化路径继续使用原有SEND/RECV。

### READ 源数据按控制批次持有

每个 StartReadTransfer 成功接受的前缀对应一个 SourceContext 和一个 versioned source ID，持有这一批全部源 IOBuf 引用。PULL 内的 item 共用 source ID，地址、长度和 seq 各自独立。一次调用达到描述符上限时，未接受的输入留待后续批次，不包含在当前 source 中。

对端整批 READ 完成后发送一个 FREE item，携带批次 source ID、首 seq 和总长度；发送端校验 Socket/connection、首 seq 和完整长度后一次释放该 SourceContext。局部 FREE 不能提前释放源数据。PULL 入队失败时由未发布 guard 立即释放 context；入队成功后必须等匹配的 FREE，不能按本地 SEND CQE 提前回收。

sources_outstanding 统计在途源批次数，不再统计 item 数。二进制布局不变，但 PULL/FREE 的 source ID 语义调整为批次，两端需一起更新。
