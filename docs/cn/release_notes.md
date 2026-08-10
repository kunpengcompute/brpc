# 版本说明书

## 版本配套说明

### 产品版本信息

<a name="table62675726"></a>
<table><tbody><tr id="row41561572"><th class="firstcol" valign="top" width="42.17%" id="mcps1.1.3.1.1"><p id="p11044137"><a name="p11044137"></a><a name="p11044137"></a>产品名称</p>
</th>
<td class="cellrowborder" valign="top" width="57.830000000000005%" headers="mcps1.1.3.1.1 "><p id="p1597721693713"><a name="p1597721693713"></a><a name="p1597721693713"></a>Kunpeng BoostKit</p>
</td>
</tr>
<tr id="row24726251"><th class="firstcol" valign="top" width="42.17%" id="mcps1.1.3.2.1"><p id="p56669300"><a name="p56669300"></a><a name="p56669300"></a>产品版本</p>
</th>
<td class="cellrowborder" valign="top" width="57.830000000000005%" headers="mcps1.1.3.2.1 "><p id="p11923034"><a name="p11923034"></a><a name="p11923034"></a><span id="text14311218114"><a name="text14311218114"></a><a name="text14311218114"></a>26.2.RC1</span></p>
</td>
</tr>
<tr id="row1930811171892"><th class="firstcol" valign="top" width="42.17%" id="mcps1.1.3.3.1"><p id="p2030912172097"><a name="p2030912172097"></a><a name="p2030912172097"></a>软件名称</p>
</th>
<td class="cellrowborder" valign="top" width="57.830000000000005%" headers="mcps1.1.3.3.1 "><p id="p1730912179911"><a name="p1730912179911"></a><a name="p1730912179911"></a><span id="text17191017111119"><a name="text17191017111119"></a><a name="text17191017111119"></a>bRPC</span></p>
</td>
</tr>
<tr id="row1930811171892"><th class="firstcol" valign="top" width="42.17%" id="mcps1.1.3.3.1"><p id="p2030912172097"><a name="p2030912172097"></a><a name="p2030912172097"></a>软件版本</p>
</th>
<td class="cellrowborder" valign="top" width="57.830000000000005%" headers="mcps1.1.3.3.1 "><p id="p1730912179911"><a name="p1730912179911"></a><a name="p1730912179911"></a><span id="text17191017111119"><a name="text17191017111119"></a><a name="text17191017111119"></a>V1.0.0</span></p>
</td>
</tr>
</tbody>
</table>

### 与操作系统、编译器和CPU配套说明

|操作系统|CPU类型|编译器|
|--|--|--|
|openEuler 24.03|鲲鹏950处理器|g++-12|

## 版本更新说明

### V1.0.0

**新增特性**

|特性描述|更新说明|
|--|--|
|支持MEMFD共享内存通信|新增MEMFD传输模式，支持同一主机内的客户端与服务端通过共享内存队列传输数据，减少数据复制和内核协议栈开销。新增`SOCKET_MODE_MEMFD`及共享内存分配、会话管理、FD传递和零拷贝序列化能力；MEMFD模式仅适用于本机通信，并由框架将本地IP地址自动转换为对应的Unix域套接字地址。|
|支持输入消息批处理|新增输入消息批量调度和按连接自适应调整能力，TCP、RDMA和MEMFD传输均可使用。通过`--input_message_batch_process_size`配置：`0`或`1`保持逐消息处理，`>1`按指定上限批量处理，`-1`根据连接近期读取的消息突发量在`1/2/4/8/16`档位间自适应调整；批次在当前读循环结束时及时提交，不等待凑满。|

## 版本配套文档

### 版本配套文档

| 文档名称 | 内容简介 | 交付形式 |
| --------- | --------- | ---- |
| 快速入门 | 提供bRPC新增特性使能的快速入门指导。 | 开源仓 |
| 版本说明书 | 提供bRPC仓发布版本的基础信息和特性更新信息。 | 开源仓 |
| API参考 | 提供接口说明、接口调用示例等。 | 开源仓 |

### 获取文档的方法

您可以通过访问[开源仓](https://gitcode.com/boostkit/brpc/tree/dev_brpc_930)浏览和获取相关文档。
