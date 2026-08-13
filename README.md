# bRPC介绍

## 最新消息

- [2026-09-30]：以分支`dev_brpc_930`commit `4388c277ba5d731041aefbf837c71cd9cd2d5a65`为基线发布补丁仓v1.0.0版本，支持bRPC框架优化。

## 项目介绍

bRPC是用C++语言编写的工业级RPC框架，常用于搜索、存储、机器学习、广告、推荐等高性能系统。


## 目录结构

主要目录如下：

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

## 版本说明

每个发布版本特性变更详细信息，请参见《[版本说明书](docs/zh/release_notes.md)》。

## 快速上手

bRPC新增特性使能的详细步骤，请参见《[快速入门](docs/zh/quick_start.md)》。

## 文档

| 资源名称 | 资源简介 |
| --------- | --------- |
| [快速入门](./docs/zh/quick_start.md) | 提供bRPC的快速入门指导。 |
| [版本说明书](./docs/zh/release_notes.md) | 提供bRPC发布版本的基础信息和特性更新信息。 |
| [API参考](./docs/zh/api_reference.md) | 提供接口说明、接口调用示例等。 |

## 免责声明

此代码仓计划参与bRPC软件开源，仅对bRPC部分功能进行性能优化。其中编码风格遵照原生开源软件，继承原生开源软件安全设计，不破坏原生开源软件设计及编码风格和方式。软件的任何漏洞与安全问题，均由相应的上游社区根据其漏洞和安全响应机制解决。请密切关注上游社区发布的通知和版本更新。鲲鹏计算社区对软件的漏洞及安全问题不承担任何责任。

## License

bRPC仓遵循 Apache-2.0许可证，具体请参见[LICENSE文件](LICENSE)。

本项目的文档适用CC-BY 4.0许可证，具体请参见[LICENSE文件](docs/LICENSE)。

## 贡献指南

如果使用过程中有任何问题，或者需要反馈特性需求和bug报告，可以提交isssues联系我们，具体贡献方法可参考[这里](https://gitcode.com/boostkit/community/blob/master/docs/contributor/contributing.md)。

## 建议与交流

欢迎大家为社区做贡献。如果有任何疑问或建议，请提交[Issues](https://gitcode.com/boostkit/community/blob/master/docs/contributor/issue-submit.md)，我们会尽快回复。感谢您的支持。

## 致谢

bRPC仓由华为公司的下列部门联合贡献：

鲲鹏计算Boostkit开发部

感谢来自社区的每一个PR，欢迎贡献bRPC仓！
