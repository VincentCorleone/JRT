# 比赛相关

## 本地模式和集群模式需要修改的
pom里面的scope

setNamesrvAddr

提交拓扑时不使用LocalCluster

RaceTopology的MAIN函数里面不启动生产者

tair配置项

## 官方demo地址
https://code.aliyun.com/MiddlewareRace/PreliminaryDemo


## 重要链接

[阿里中间件性能挑战赛重要学习资料](https://bbs.aliyun.com/read/277544.html?spm=5176.100068.555.2.kYJDZ2)

[代码提交注意事项](https://bbs.aliyun.com/read/286553.html?spm=5176.bbsl254.0.0.ld0DpD)

[试跑说明](https://bbs.aliyun.com/read/287111.html?spm=5176.bbsl254.0.0.)

# 学习路径

## Maven
清空编译结果:
```bash
mvn clean
```
编译成class文件并运行(com.scut.gxc.SimpleTopology为入口类):
```bash
mvn compile -e
mvn exec:java -Dexec.mainClass="com.alibaba.middleware.race.jstorm.RaceTopology"
```
编译成jar包:
```
mvn assembly:assembly -e
```
官方打包命令
```
mvn clean -f ./pom.xml assembly:assembly  -Dmaven.test.skip=true
```
官方运行jar包命令
```
jstorm jar preliminary.demo-1.0-SNAPSHOT.jar com.alibaba.middleware.race.jstorm.RaceTopology
```
## Jstorm

### 官方帮助文档

[jstorm帮助文档](https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation?spm=5176.100068.555.3.kYJDZ2)


### 知识准备

[Storm学习(一)Storm介绍](http://blog.csdn.net/lifuxiangcaohui/article/details/40651373)

[JStorm组件方法解释](http://www.open-open.com/lib/view/open1430095563146.html)

[JStorm 5分钟基础概念](https://github.com/alibaba/jstorm/wiki/JStorm-basics-in-5-min)

[基本概念](https://github.com/alibaba/jstorm/wiki/%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5)


### 实战

[安装](https://github.com/alibaba/jstorm/wiki/%E5%A6%82%E4%BD%95%E5%AE%89%E8%A3%85)

[如何本地调试 JStorm 程序](https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation?spm=5176.100068.555.3.kYJDZ2)

[storm入门——本地模式helloworld](http://www.cnblogs.com/zhangyukun/p/4031066.html)

[最简单的JStorm例子](https://github.com/alibaba/jstorm/wiki/%E5%BA%94%E7%94%A8%E4%BE%8B%E5%AD%90)

[Storm杂记 — Field Grouping和Shuffle Grouping的区别](http://blog.csdn.net/luonanqin/article/details/40436397)

> execute方法从bolt的一个输入接收tuple(一个bolt可能有多个输入源). ExclamationBolt获取tuple的第一个字段，加上”!!!”之后再发射出去。如果一个bolt有多个输入源，你可以通过调用Tuple#getSourceComponent方法来知道它是来自哪个输入源的。


摘自[storm 入门原理介绍 ](http://blog.itpub.net/29754888/viewspace-1260026/)

将特定tuple发送给特定Bolt的方法：
[Strom数据流分组解析](http://blog.csdn.net/dlf123321/article/details/51811965)

### 笔记摘录

+ Nimbus：负责资源分配和任务调度。
+ Supervisor：负责接受nimbus分配的任务，启动和停止属于自己管理的worker进程。
+ Worker：运行具体处理组件逻辑的进程。
+ Task：worker中每一个spout/bolt的线程称为一个task. 在storm0.8之后，task不再与物理线程对应，同一个spout/bolt的task可能会共享一个物理线程，该线程称为executor。

![几个角色之间的关系](http://www.searchtb.com/wp-content/uploads/2012/08/deploy0.jpg)


+ Topology：storm中运行的一个实时应用程序，因为各个组件间的消息流动形成逻辑上的一个拓扑结构。
+ Spout：在一个topology中产生源数据流的组件。通常情况下spout会从外部数据源中读取数据，然后转换为topology内部的源数据。Spout是一个主动的角色，其接口中有个nextTuple()函数，storm框架会不停地调用此函数，用户只要在其中生成源数据即可。
+ Bolt：在一个topology中接受数据然后执行处理的组件。Bolt可以执行过滤、函数操作、合并、写数据库等任何操作。Bolt是一个被动的角色，其接口中有个execute(Tuple input)函数,在接受到消息后会调用此函数，用户可以在其中执行自己想要的操作。
+ Tuple：一次消息传递的基本单元。本来应该是一个key-value的map，但是由于各个组件间传递的tuple的字段名称已经事先定义好，所以tuple中只要按序填入各个value就行了，所以就是一个value list.
+ Stream：源源不断传递的tuple就组成了stream。
+ stream grouping：即消息的partition方法。Storm中提供若干种实用的grouping方式，包括shuffle, fields hash, all, global, none, direct和localOrShuffle等

+ storm的topology中的一个系统级组件，acker的任务就是追踪从spout中流出来的每一个message id绑定的若干tuple的处理路径，如果在用户设置的最大超时时间内这些tuple没有被完全处理，那么acker就会告知spout该消息处理失败了，相反则会告知spout该消息处理成功了。在刚才的描述中，我们提到了”记录tuple的处理路径”，如果曾经尝试过这么做的同学可以仔细地思考一下这件事的复杂程度。但是storm中却是使用了一种非常巧妙的方法做到了。在说明这个方法之前，我们来复习一个数学定理。
>A xor A = 0.
>
>A xor B…xor B xor A = 0，其中每一个操作数出现且仅出现两次。

以上内容来自[Storm学习(一)Storm介绍](http://blog.csdn.net/lifuxiangcaohui/article/details/40651373)


## RocketMQ

### 官方帮助文档

[RocketMQ 开发帮助文档(其实里面没啥东西)](https://github.com/alibaba/RocketMQ)

[RocketMQ用户指南](pdf/RocketMQ_userguide.pdf)

[RocketMQ最佳实践](pdf/RocketMQ_experience.pdf)

[RocketMQ原理简介](pdf/RocketMQ_design.pdf)

### 启动和关闭命令

启动nameserver:
```bash
nohup mqnamesrv 1>/root/software/alibaba-rocketmq/log/ng.log 2>/root/software/alibaba-rocketmq/log/ng-err.log &
```
启动broker：
```bash
nohup mqbroker >/root/software/alibaba-rocketmq/log/mq.log &
```
关闭nameserver:

```bash
mqshutdown namesrv
```

关闭broker：

```bash
mqshutdown broker
```

### 知识准备

[分布式开放消息系统(RocketMQ)的原理与实践](http://www.jianshu.com/p/453c6e7ff81c)(原理部分可先跳过，重点看实践部分)

### 实战

[RocketMQ 消息队列单机部署及使用](http://blog.csdn.net/loongshawn/article/details/51086876)

[jstorm和rocketMQ组合使用](https://github.com/alibaba/jstorm/tree/master/jstorm-utility/jstorm-rocket-mq)

### 笔记摘录

Producer:

消息生产者，负责产生消息，一般由业务系统负责产生消息。

Consumer:

消息消费者，负责消费消息，一般是后台系统负责异步消费。

Consumer Group:

一类Consumer的集合名称，这类Consumer通常消费一类消息，且消费逻辑一致。

Broker:

消息中转角色，负责存储消息，转发消息，一般也称为Server。在JMS规范中称为Provider。

广播消费:

一条消息被多个Consumer消费，即使这些Consumer属于同一个Consumer Group，消息也会被Consumer Group中的每个Consumer都消费一次。
在CORBA Notification规范中，消费方式都属于广播消费。

集群消费:

一个Consumer Group中的Consumer实例平均分摊消费消息，类似于JMS规范中的Point-to-Point Messaging
特点如下：

+ Each message has only one consumer.
+ A sender and a receiver of a message have no timing dependencies. The receiver can fetch the message whether or not it was running when the client sent the message.
+ The receiver acknowledges the successful processing of a message.

主动消费：

Consumer主动向Broker发起获取消息请求，控制权完全在于Consumer应用。
类似于JMS规范中描述的Synchronously方式消费

被动消费：

Consumer注册一个Callback接口，由Metaq后台自动从Broker接收消息，并回调Callback接口。
类似于JMS规范中的描述的Asynchronously方式消费

## Tair

## 官方文档

[Tair开发帮助文档](http://code.taobao.org/p/tair/wiki/index/)



# 杂七杂八

- 从论坛、旺旺群收集到的有用关键字：
jstorm的并发 和 流分组策略  流控

- RocketMQ不保证消息不重复，如果你的业务需要保证严格的不重复消息，需要你自己在业务端去重。

- 尽量使用批量方式消费方式，可以很大程度上提高消费吞吐量。

- [其他参加中间件比赛的选手的博客](http://blog.csdn.net/leishenop/article/details/51626843)

- 07-03 13:45 注意： 选手依赖的Tair版本请改成2.3.5版本，jstorm默认的日志框架是logback，为了避免冲突拓扑中最好也统一用logback日志框架，像demo一样； 
　　淘宝每分钟的交易金额的key更新为platformTaobao_teamcode_整分时间戳,天猫每分钟的交易金额的key更新为platformTmall_teamcode_整分时间戳,每整分时刻无线和PC端总交易金额比值的key 更新为ratio_teamcode_整分时间戳，teamcode每个队伍的唯一标识，请知晓！

- [沈洵视频讲解](http://v.youku.com/v_show/id_XODY4ODE3OTY0.html?from=s1.8-1-1.2)


- 编译失败：
```
submit topology failed. can't find topology jar fail
```

- 编译成功但一直卡在拓扑提交
```
submitting topology.../opt/taobao/java/bin/java
[INFO  2016-07-03 18:24:51 ConfigServer:156 main] init configs from configserver: 10.101.72.127:5198
[INFO  2016-07-03 18:24:51 ConfigServer:185 main] alive datanode: 10.101.72.127:5191
[INFO  2016-07-03 18:24:51 ConfigServer:185 main] alive datanode: 10.101.72.128:5191
[WARN  2016-07-03 18:24:51 ConfigServer:210 main] configuration inited with version: 126, bucket count: 1023, copyCount: 1
[WARN  2016-07-03 18:24:51 
```

- 编译成功且提交拓扑成功但是准确率为0：
```
your topology maybe not ok, because the accuracy is zero
```

