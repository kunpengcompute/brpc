# API参考

## 接口说明

bRPC新增的MEMFD共享内存通信与输入消息批处理接口如[**表 1** 新增接口和配置项列表](#新增接口和配置项列表)所示。

**表 1** 新增接口和配置项列表<a id="新增接口和配置项列表"></a>

|接口名称|接口配置项|
|--|--|
|SocketMode|指定Socket使用TCP、RDMA或MEMFD传输模式。|
|ServerOptions::socket_mode|指定服务端侦听连接使用的传输模式。|
|ChannelOptions::socket_mode|指定客户端Channel使用的传输模式。|
|--shm_buff_size|设置MEMFD共享内存数据缓冲区大小。|
|--shm_block_size|设置MEMFD共享内存数据块大小。|
|--shm_queue_size|设置MEMFD共享内存队列映射大小。|
|--input_message_batch_process_size|设置输入消息逐条、固定批次或自适应批次处理方式。|

## 接口定义

### SocketMode

**接口功能**

指定bRPC Socket的数据传输模式。`SOCKET_MODE_MEMFD`用于同一主机内的进程间通信，框架通过Unix域套接字完成连接建立和文件描述符传递，通过MEMFD共享内存传输RPC数据。

**接口定义**

```cpp
enum SocketMode {
    SOCKET_MODE_TCP = 0,
    SOCKET_MODE_RDMA = 1,
    SOCKET_MODE_MEMFD = 2
};
```

**参数说明**

|参数|参数值|参数说明|
|--|---|--|
|SOCKET_MODE_TCP|0|使用原有TCP通信路径，为默认值。|
|SOCKET_MODE_RDMA|1|使用RDMA通信路径，需要在编译时启用RDMA支持。|
|SOCKET_MODE_MEMFD|2|使用MEMFD共享内存通信路径，仅支持同一主机内的客户端与服务端。|

**返回值**

无。

>![说明](../figures/icon-note.gif) **说明：**
>MEMFD模式要求服务端和客户端配置相同的`socket_mode`。调用方仍可使用本地IPv4地址和端口，框架会自动将其转换为名称为`brpc_memfd_<port>`的Linux抽象Unix域套接字地址，无需显式指定套接字文件。非本地IPv4地址、无效端口或不匹配的传输模式会导致连接或服务启动失败。

### ServerOptions::socket_mode

**接口功能**

设置服务端侦听连接使用的传输模式。

**接口定义**

```cpp
brpc::ServerOptions options;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;
```

**参数说明**

|参数|参数说明|取值范围|输入/输出|
|--|--|--|--|
|socket_mode|服务端传输模式。|SOCKET_MODE_TCP、SOCKET_MODE_RDMA、SOCKET_MODE_MEMFD，默认值为SOCKET_MODE_TCP。|输入|

**返回值**

该字段本身无返回值。`Server::Start()`成功时返回`0`，初始化传输层或侦听失败时返回`-1`。

**示例**

```cpp
#include "brpc/server.h"

brpc::Server server;
brpc::ServerOptions options;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;

if (server.Start(10086, &options) != 0) {
    return -1;
}
```

服务端会将逻辑地址的端口`10086`映射到Linux抽象Unix域套接字`brpc_memfd_10086`。抽象套接字不生成文件系统路径，服务退出后无需删除套接字文件。

### ChannelOptions::socket_mode

**接口功能**

设置客户端Channel使用的传输模式。

**接口定义**

```cpp
brpc::ChannelOptions options;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;
```

**参数说明**

|参数|参数说明|取值范围|输入/输出|
|--|--|--|--|
|socket_mode|客户端传输模式。|SOCKET_MODE_TCP、SOCKET_MODE_RDMA、SOCKET_MODE_MEMFD，默认值为SOCKET_MODE_TCP。|输入|

**返回值**

该字段本身无返回值。`Channel::Init()`成功时返回`0`，传输层初始化或连接参数校验失败时返回`-1`。MEMFD目标不是本机地址时，Channel创建连接或发起RPC将失败。

**示例**

```cpp
#include "brpc/channel.h"

brpc::Channel channel;
brpc::ChannelOptions options;
options.protocol = brpc::PROTOCOL_BAIDU_STD;
options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;

if (channel.Init("127.0.0.1:10086", &options) != 0) {
    return -1;
}
```

示例中的`127.0.0.1:10086`是逻辑服务地址。MEMFD模式下，框架会判断该地址是否属于本机，并自动连接到对应的抽象Unix域套接字。

## MEMFD配置项

### --shm_buff_size

**参数功能**

设置当前进程创建的MEMFD共享内存数据缓冲区总大小。该缓冲区由进程内的共享内存块分配器统一管理，RPC数据超过单个块的有效容量时会占用多个块。

**参数定义**

```text
--shm_buff_size=<字节数>
```

**参数说明**

|参数|参数类型|默认值|取值要求|
|--|--|--|--|
|shm_buff_size|无符号64位整数|1073741824（1 GiB）|必须能够容纳共享内存头、块状态信息和至少一个数据块。|

缓冲区越大，可同时保留的未释放共享内存数据越多，但进程的虚拟内存映射也越大。该参数应在首次初始化MEMFD传输之前设置。

### --shm_block_size

**参数功能**

设置MEMFD数据缓冲区的块大小。共享内存分配、回收和跨块零拷贝流以该值作为基本粒度。

**参数定义**

```text
--shm_block_size=<字节数>
```

**参数说明**

|参数|参数类型|默认值|取值要求|
|--|--|--|--|
|shm_block_size|有符号64位整数|8192（8 KiB）|必须大于sizeof(butil::IOBuf::Block)，且shm_buff_size必须足以分配至少一个块。|

较小的块可以降低小消息的内部空间浪费，但会增加大消息跨块时的元数据和管理开销；较大的块可以减少大消息的块数量，但可能增加小消息的空间占用。

### --shm_queue_size

**参数功能**

设置每个MEMFD发送队列的共享内存映射大小。队列保存数据块偏移和数据长度等描述信息，不直接保存RPC正文。

**参数定义**

```text
--shm_queue_size=<字节数>
```

**参数说明**

|参数|参数类型|默认值|取值要求|
|--|--|--|--|
|shm_queue_size|无符号64位整数|1048576（1 MiB）|必须至少能够容纳一个队列头和一个队列元素。|

队列较小时，高并发发送更容易触发队列满和反压。队列较大时，每条连接对应的共享内存映射开销会增加。

**命令行示例**

```bash
./server \
    --shm_buff_size=1073741824 \
    --shm_block_size=8192 \
    --shm_queue_size=1048576
```

>![说明](../figures/icon-note.gif) **说明：**
>`shm_buff_size`是进程级数据缓冲区大小，`shm_block_size`是数据分配粒度，`shm_queue_size`是队列描述符区域大小，三者用途不同。客户端和服务端应分别根据各自发送方向的并发量和消息大小配置。生产环境修改这些参数前应使用实际业务消息大小和并发度进行压测。

## 输入消息批处理配置项

### --input_message_batch_process_size

**参数功能**

控制一次读取中解析出的输入消息如何调度。启用批处理后，一个bthread会按解析顺序依次处理同一批消息，从而减少高并发场景下创建和调度bthread的开销。该参数对进程内接收的消息生效，因此客户端响应和服务端请求可分别配置。

**参数定义**

```text
--input_message_batch_process_size=<批次大小>
```

**参数说明**

|参数取值|处理方式|
|--|--|
|-1|启用按Socket自适应批处理。框架根据近期每次读取解析出的可处理消息数，在`1/2/4/8/16`五个档位间调整有效批次。|
|0|保持原有逐消息调度行为，为默认值。|
|1|保持逐消息调度行为，用于显式关闭批处理。|
|>1|使用指定数值作为固定批次上限。|
|<-1|非法取值，参数校验失败。|

自适应模式按连接独立维护历史状态：负载上升时每次最多提升一个档位，负载下降时可快速回落；空读和不完整消息不参与统计。固定批次和自适应批次均只处理当前读循环已经解析出的消息，批次未满时也会在读循环结束前提交，不会等待后续消息凑满。

当启用`--usercode_in_coroutine`时，框架保持逐消息处理。渐进式读取以及RDMA polling等要求消息立即移交的路径不会聚合批次。

**返回值**

无。参数值小于`-1`时，gflags校验失败，进程不会使用该配置启动。

**命令行示例**

使用自适应批处理：

```bash
./server --input_message_batch_process_size=-1
```

使用固定大小为8的批处理：

```bash
./server --input_message_batch_process_size=8
```

关闭批处理并保持原有处理路径：

```bash
./server --input_message_batch_process_size=0
```

>![说明](../figures/icon-note.gif) **说明：**
>该配置表示一次bthread调度可顺序处理的消息数量上限，不改变RPC协议、消息顺序和业务处理语义。建议先使用`-1`进行自适应调节，并通过实际业务压测比较QPS和尾延迟；低并发或单请求延迟敏感场景可保持默认值`0`。

## MEMFD与消息批处理组合示例

服务端配置：

```cpp
brpc::ServerOptions options;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;
server.Start(10086, &options);
```

客户端配置：

```cpp
brpc::ChannelOptions options;
options.socket_mode = brpc::SOCKET_MODE_MEMFD;
options.protocol = brpc::PROTOCOL_BAIDU_STD;
channel.Init("127.0.0.1:10086", &options);
```

分别启动客户端和服务端进程时，可在两端独立启用自适应消息批处理：

```bash
./server --input_message_batch_process_size=-1
./client --input_message_batch_process_size=-1
```

MEMFD负责数据传输，输入消息批处理负责解析后的调度，两项功能相互独立。只配置`SOCKET_MODE_MEMFD`不会自动开启消息批处理；不使用MEMFD时，TCP和RDMA输入消息也可以使用批处理功能。
