# Bazel编译brpc获取可执行用例server、client
## 1. 支持gflags参数说明：


| 名称 | 含义 | 取值范围 | 默认值 | 必填 |
|--|--|--|--|--|
| ubsocket_trans_mode | 通信协议 |ub，ib  | ub | 否 |
ubsocket_dev_name|设备名称|根据实际场景填写设备名称；例如，udma2或者bonding_dev_0|主动获得当前环境bonding设备名称，如bonding_dev_xx|否
ubsocket_eid_idx|使用普通设备的eid编号|ub协议下，通过urma_admin show命令查询获得|0|否
ubsocket_src_eid|使用bonding设备的eid|ub协议下，通过urma_admin show命令查询获得|主动获得当前环境bonding设备的eid|否
ubsocket_log_level|日志级别|emerg，alert，crit，err，warn，notice，info，debug|info|否
ubsocket_log_use_printf|是否将日志打印到前台|0，1|0|否
ubsocket_tx_depth|发送队列深度|最小值是2，设置上限由实际机器环境决定（根据命令urma_admin show --whole中max_jfc_depth与max_jfs_depth两者的最小值）|1024|否
ubsocket_rx_depth|接受队列深度|最小值是2，设置上限由实际机器环境决定（根据命令urma_admin show --whole中max_jfc_depth与max_jfr_depth两者的最小值）|1024|否
ubsocket_readv_unlimited|是否打开readv上报限制|false，true|true|否
ubsocket_block_type|内存池的最小分片|default，large|default|否
ubsocket_pool_initial_size|IO内存的总大小，单位MB|应用按需配置|1024|否
ubsocket_ub_force|是否强制使用UB协议加速TCP|0：不强制用UB加速TCP 1：强制用UB加速TCP|0|否
ubsocket_schedule_policy|设置多平面负载分担策略|affinity，rr|affinity|否
ubsocket_auto_fallback_tcp|	协议不匹配时是否自动降级为TCP|0：协议不匹配时不降级为TCP 1：协议不匹配时自动降级为TCP|1|否
ubsocket_trace_enable | 是否打开trace统计|false, true | false |否
ubsocket_trace_time | 控制维测数据输出间隔（单位s）| [1, 300] | 10 |否
ubsocket_trace_file_path | 控制维测数据输出路径| [1, 512] | /tmp/ubsocket/log |否
ubsocket_trace_file_size | 控制维测数据文件大小（MB）| [1, 300] | 10 |否

## 2：bazel编译
- 安装基础软件
执行以下命令安装各步骤所需要的基础软件。
```
$ yum install gcc-toolset-14-* zip vim tar unzip cmake make -y
$ yum install patchelf perl hdf5-devel -y
$ yum install python python3-pip python3-devel wget git -y
$ yum install automake libtool -y
```
安装完成后，需配置gcc相关的环境变量，并确认gcc是否正确安装
```
$ export PATH=/opt/openEuler/gcc-toolset-14/root/usr/bin/:$PATH
$ export LD_LIBRARY_PATH=/opt/openEuler/gcc-toolset-14/root/usr/lib64/:$LD_LIBRARY_PATH
$ # 通过gcc -v确认gcc是否正确安装，预期显示"gcc version 14.x.x"
$ gcc -v
```
- bazel下载与编译
推荐下载bazel 7.4.1，直接下载可执行文件即可。
```
$ wget https://github.com/bazelbuild/bazel/releases/download/7.4.1/bazel-7.4.1-linux-arm64 --no-check-certificate
$ chmod +x bazel-7.4.1-linux-arm64
$ cp bazel-7.4.1-linux-arm64 /usr/local/bin/bazel
```
- 下载bRPC源码
通过git clone方式下载bRPC源码，并确保切换到目标的分支或tag。
```
$ git clone https://gitcode.com/fanzhaonan/brpc.git
$ git checkout br_noncom_ub_20251215
```

### 如何修改版本tag
在brpc根目录的WORKSPACE中找到ubsocket依赖，如下：
```
 git_repository(
 	 	name = "ubsocket",
 	 	remote = "https://atomgit.com/openeuler/ubs-comm",
 	 	commit = "c8076b68d5b27cd5f0ef5a98ede765e06130d6e4",
 	 )
     
```
>说明：修改commit值（切换版本后，提交记录左边有最新commit值，显示简版8位字符串，详细40位，想要对应版本，修改对应提交的commit值即可）

### 执行编译   
bazel支持自动检测依赖变化，能够实现高效的增量编译，建议直接编译可执行文件。如可通过如下命令，编译brpc示例中的echo_c++。
>说明：如果配置代理，需要配置https协议对应的证书。  
>例：在根目录下vim  .bazelrc,在最下面加上如下配置  
>startup --host_jvm_args=-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/   (证书路径)   
>startup --host_jvm_args=-Djavax.net.ssl.trustStorePassword=changeit  
>startup --host_jvm_args=-DBAZEL_TRACK_SOURCE_DIRECTORIES=1

```
$ cd  brpc  #在根目录下执行编译命令
$ bazel build //example:echo_c++_server  # 编译服务端，编译产物在 brpc/bazel-bin/example
$ bazel build //example:echo_c++_client  # 编译客户端，编译产物在 brpc/bazel-bin/example
```
>说明：  
>在bazel编译的时候 后面添加--compilation_mode=opt（或简写-c opt），可以自动设置高级别的编译器优化选项（如-O2），能够提高性能，适合发布版本场景

当然，也可以根据需要仅编译出libbrpc.a，后续在用该静态库编译可执行文件。
```
$ cd  brpc  #在根目录下执行编译命令
$ bazel build --noenable_bzlmod --distdir=/root/proxy :brpc # 编译产物在 brpc/bazel-bin中
```
> 说明：
>编译参数“--noenable_bzlmod ”表示不使用bzlmod特性，主要依赖在WORKSPACE中已添加，
>编译参数“--distdir=/root/proxy ”指定下载路径，WORKSPACE中的相关三方依赖，例:protobuf 5.28.3、gflags 2.2.2、leveldb 1.23、openssl 1.1.1m等会自动下载到该路径，可以根据需要配置。

- 编译用例执行示例
上述完成后可以获得echo_c++_server和echo_c++_client两个可执行文件，分别放到放到两台服务器上
```
$ # 将echo_c++_server和echo_c++_client放到两台服务器上
$ # 启动echo_c++_server
$ ./echo_c++_server --ubsocket_log_use_printf=1 --ubsocket_ub_force=1

$ # 在另一个节点启动echo_c++_client
$ ./echo_c++_client --server=141.61.85.60:8000  --ubsocket_log_use_printf=1 --ubsocket_ub_force=1
```
>说明：参数根据具体服务器配置进行调整，参数详情参考上面gflags参数。
>
>启动命令不添加ubsocket_ub_force 参数，需要在client.cpp和server.cpp源码main方法中，添加“options.use_ub = FLAGS_use_ub;"
