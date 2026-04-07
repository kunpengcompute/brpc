# bazel编译带UB能力的bRPC

## 1 概述

### 1.1 简介

基于[社区bPRC](https://github.com/apache/brpc/)进行修改，支持UB通信，提升RPC性能。

### 1.2 工作原理

[openEuler/ubs-comm](https://atomgit.com/openeuler/ubs-comm)中的ubsocket组件，支持拦截TCP应用中的POSIX Socket API，将TCP通信转换为UB高性能通信。bRPC通过对接ubsocket实现UB通信。

## 2 配套说明

| 软件名称       | 软件版本                                      |
| :------------- | -------------------------------------------- |
| OS             | 含UB OS Component的OS（OpenEuler-24.03-sp3） |
| GCC            | 14.3.1/13.2.0                                |
| Bazel          | 7.4.1                                        |
| protobuf       | 33.0                                         |
| glibc          | v2.34                                        |
| gflags         | 2.2.2                                        |
| gperftools     | 2.17.2                                       |
| leveldb        | 1.23                                         |
| glog           | 0.5.0                                        |
| zlib           | 1.3.1                                        |
| boringssl      | c00d7ca810e3780bd0c8ee4eea28f4f2ea4bcdc      |

## 3 API说明

ubsocket通过环境变量进行配置，在bRPC与ubsocket集成的过程中，为了保持bRPC的使用习惯，将ubsocket的环境变量配置项全部转换成了bRPC的gflags配置项。gflags配置项见下表。

| 名称 | 含义 | 取值范围 | 默认值 | 必填 |
|--|--|--|--|--|
| ubsocket_trans_mode | 通信协议 |ub，ib  | ub | 否 |
|ubsocket_dev_name|设备名称|根据实际场景填写设备名称；例如，udma2或者bonding_dev_0|主动获得当前环境bonding设备名称，如bonding_dev_xx|否|
|ubsocket_eid_idx|使用普通设备的eid编号|ub协议下，通过urma_admin show命令查询获得|0|否|
|ubsocket_src_eid|使用bonding设备的eid|ub协议下，通过urma_admin show命令查询获得|主动获得当前环境bonding设备的eid|否|
|ubsocket_log_level|日志级别|error，warn，notice，info，debug|info|否|
|ubsocket_log_use_printf|是否将日志打印到前台|false，true|true|否|
|ubsocket_tx_depth|发送队列深度|最小值是64，设置上限由实际机器环境决定（根据命令urma_admin show --whole中max_jfc_depth与max_jfs_depth两者的最小值）|1024|否|
|ubsocket_rx_depth|接受队列深度|最小值是64，设置上限由实际机器环境决定（根据命令urma_admin show --whole中max_jfc_depth与max_jfr_depth两者的最小值）|1024|否|
|ubsocket_readv_unlimited|是否打开readv上报限制|false，true|true|否|
|ubsocket_block_type|内存池的最小分片|default：8k，small：16k，medium：32k，large：64k|default|否|
|ubsocket_pool_initial_size|IO内存的总大小，单位MB|应用按需配置|1024|否|
|ubsocket_schedule_policy|设置多平面负载分担策略|affinity_priority, affinity，rr|affinity_priority|否|
|ubsocket_auto_fallback_tcp|	协议不匹配时是否自动降级为TCP|false：协议不匹配时不降级为TCP true：协议不匹配时自动降级为TCP|true|否|
|ubsocket_trace_enable | 是否打开trace统计|false, true | true |否|
|ubsocket_trace_time | 控制维测数据输出间隔（单位s）| [1, 300] | 10 |否|
|ubsocket_trace_file_path | 控制维测数据输出路径| [1, 512] | /tmp/ubsocket/log |否|
|ubsocket_trace_file_size | 控制维测数据文件大小（MB）| [1, 300] | 10 |否|
|ubsocket_stats_cli | 是否打开cli服务|false, true | true |否|
|ubsocket_enable_share_jfr|	设置是否开启共享JFR|false：不开启共享jfr<br>true：开启共享jfr|true|否|
|ubsocket_share_jfr_rx_queue_depth|设置开启共享JFR后，每个Socket链接接收缓存队列深度|最小值是64，设置上限由实际机器环境决定|1024|否|
|ubsocket_link_priority|设置URMA流量SL优先级|[0, 15]|0|否|
|ubsocket_ub_trans_mode | ub协议模式 |RC_TP，RM_TP, RM_CTP, RC_CTP  | RC_TP | 否 |
|`ubsocket_enable` | 是否启用 ubsocket 加速 | false, true | false | 否 |

> 注意：
>
> 为最大程度的兼容bPRC及gflags的使用习惯。新增的这些gflags配置项，均在bRPC的源码中指定了默认值。bRPC集成ubsocket的场景中，ubsocket自身的环境变量不再生效，以gflags的默认值或用户指定的gflags值为准。

## 4 bazel编译

### 4.1 安装基础软件

执行以下命令安装各步骤所需要的基础软件。
```
$ yum install gcc-toolset-14-* zip vim tar unzip cmake make -y
$ yum install patchelf perl hdf5-devel -y
$ yum install python python3-pip python3-devel wget git -y
$ yum install automake libtool -y
```
安装完成后，需配置gcc相关的环境变量，并确认gcc是否正确安装
```
$ # 使用gcc14自带的脚本，完成PATH和LD_LIBRARY_PATH等环境变量配置
$ source /opt/openEuler/gcc-toolset-14/enable
$ # 通过gcc -v确认gcc是否正确安装
$ gcc -v
```
![image](../images/gcc.png)
如图显示"gcc version 14.x.x"，表明安装成功

接下来继续安装bazel，推荐直接下载bazel 7.4.1可执行文件。
```
$ wget https://github.com/bazelbuild/bazel/releases/download/7.4.1/bazel-7.4.1-linux-arm64 --no-check-certificate
$ chmod +x bazel-7.4.1-linux-arm64
$ cp bazel-7.4.1-linux-arm64 /usr/local/bin/bazel
```
### 4.2 下载bRPC源码

通过git clone方式下载bRPC源码，并确保切换到目标分支或tag。
```
$ git clone https://gitcode.com/fanzhaonan/brpc.git
$ cd brpc && git branch
```
![image](../images/brpc.png)

默认下载仓库仅存在master分支，如需其他分支可通过如下命令：
```
$ # 列出所有远程分支
$ git branch -a
$ # 创建并切换到本地分支 git checkout -b [分支名] origin/[分支名]
$ git checkout -b develop origin/develop
```
![image](../images/branch_develop.png)

本地新增develop分支表示创建成功，其他分支类似

### 4.3 修改使用的ubs-comm版本
通过修改ubs-comm的commit-id，可以指定ubs-comm版本。在brpc根目录的local_deps_ext.bzl中找到ubsocket依赖，修改commit-id值即可（使用简版8位字符串或详细40位字符串的commit-id均可）。
```
 git_repository(
		name = "ubsocket",
		remote = "https://atomgit.com/openeuler/ubs-comm",
		commit = "c8076b68d5b27cd5f0ef5a98ede765e06130d6e4",
	 )

```
![image](../images/repos_ubsocket.png)
>说明：
>
>- remote字段值一般不作修改，表示ubsocket仓库链接。

### 4.4 编译

bazel支持自动检测依赖变化，能够实现高效的增量编译，建议直接编译可执行文件。如可通过如下命令，编译brpc示例中的echo_c++。
>说明：如果配置代理，需要配置https协议对应的证书。
>例：在代码根目录下`vim .bazelrc`，在最下面加上如下配置
>startup --host_jvm_args=-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/[证书路径]
>startup --host_jvm_args=-Djavax.net.ssl.trustStorePassword=changeit
>startup --host_jvm_args=-DBAZEL_TRACK_SOURCE_DIRECTORIES=1

```
$ cd brpc  #在代码根目录下执行编译命令
$ bazel build -c opt //example:echo_c++_server --define brpc_with_urma=true  # 编译服务端，编译产物在 brpc/bazel-bin/example
$ bazel build -c opt //example:echo_c++_client --define brpc_with_urma=true  # 编译客户端，编译产物在 brpc/bazel-bin/example
```
![image](../images/bazel_bin_example.png)
>说明：
>1.在bazel编译的时候 后面添加`--define brpc_with_urma=true` 使能ubsocket，默认不使能；
>2.在bazel编译的时候 后面添加`--compilation_mode=opt`（或简写`-c opt`），可以自动设置高级别的编译器优化选项（如-O2），能够提高性能，适合发布版本场景

当然，也可以根据需要仅编译出libbrpc.a，后续再用该静态库编译可执行文件。
```
$ cd brpc  #在根目录下执行编译命令
$ bazel build --noenable_bzlmod --distdir=/root/proxy :brpc # 编译产物在 brpc/bazel-bin中
```
![image](../images/bazel_bin.png)
> 说明：
>编译参数“--noenable_bzlmod ”表示不使用bzlmod特性，主要依赖在WORKSPACE中已添加；
>编译参数“--distdir=/root/proxy ”指定下载路径，WORKSPACE中的相关三方依赖，例:protobuf、gflags、leveldb等会自动下载到该路径，可以根据需要配置。

编译ub_performance测试工具

```
$ cd brpc  #在根目录下执行编译命令
$ bazel build -c opt //example:ub_performance_server --define brpc_with_urma=true
$ bazel build -c opt //example:ub_performance_client --define brpc_with_urma=true
```

### 4.5 执行用例

上述完成后可以获得echo_c++_server和echo_c++_client两个可执行文件，分别放到放到两台服务器上
```
$ # 启动echo_c++_server
$ ./echo_c++_server --ubsocket_enable=true --use_ub=true

$ # 在另一个节点启动echo_c++_client
$ ./echo_c++_client --server=141.61.85.60:8000 --ubsocket_enable=true --use_ub=true
```
![image](../images/echo_server.png)
![image](../images/echo_client.png)
echo_c++用例执行成功，server与client互发“hello_world”
>说明：
>
>- 根据服务器实际情况，调整gflags参数，详见本文gflags介绍。
>- ubsocket为每个线程做了`thread local cache`提升性能（每个线程需额外占用内存），另外超过可用CPU核数的线程不会实际并发起来，故建议根据实际需要配置bRPC线程数以合理使用资源。参考[worker线程数](server.md#worker线程数)进行配置即可。

ub_performance测试工具执行方法

```
$ # 启动ub_performance_server
$ ./ub_performance_server --use_ub=true --ubsocket_enable=true

$ # 在另一个节点启动ub_performance_client
$ ./ub_performance_client --server=141.61.85.60:8002  --use_ub=true --ubsocket_enable=true
```