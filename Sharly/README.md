基于HMM的web异常参数检测
====
Agent
---
* 说明
>在Linux系统下监控网卡流量，提取HTTP请求数据、HTTP响应数据头，可以输出到屏幕、ES、Kafka。
* 环境
>TcpFlow
python2.7
* 使用
>详见python2.7 agent.py --help

Job
---
* 环境
>Kafka、Hadoop、ES、Spark、Zookeeper
* 使用
>1. 修改AppConfig.json，将对应的环境配置修改为自己的。

>2. 将spark依赖的jar包放到lib目录下

>3. 将python依赖的模块放到pylib下，可以是zip/egg文件，当然也可以在python解释器下安装好这些模块

>4. 在Spark Client节点使用StartJobShell.py提交任务，使用方法：python2.7 StartJobShell.py -j jobname,详见 --help

