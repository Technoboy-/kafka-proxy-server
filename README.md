#### 一. 背景
- Kafka的consumer数和parition数存在C:P的关系，如果C<=P,当某C节点挂掉，发生rebalance后，势必造成存活的某台C节点消费压力增大或导致消息消费时延增大。因此，实际的线上，C可能会部署大于P的情况以防止类似情形发生。多余的C即为standby。
另有情形下，少量的partiton数足以满足上游生产消息的速率，consumer的消费能力却成为瓶颈，因为consumer可能要处理更多的业务逻辑。如果想降低消息消费的延迟时间，只能扩大partion数量，进而增加consumer的数量，来增加消费能力，解决消息延迟性。
因此，一种代理模式就出现了，即kafka-proxy-server模式，即解决了所有线上C都可以消费到消息，又无需增大parition数来增大消费能力的情形。
- 当引入代理模式后，consumer和partiton形成解耦，且消息在proxy-server和client之间，可以加入重试队列，当消费失败后，可以在一定时间内重复收到消费失败的消息。

#### 二. 设计图

<img src="docs/static_files/proxy-server.jpeg"/>

- Proxy Server： 即原生kafka的consumer，负责某些topic消息的拉取，分发。启动后，注册实例到zookeeper中。
- 消费者： 根据配置的topic，consumerGroup，namespace，通过zookeeper，获取对应的Push Server节点地址，建立连接，并接受Proxy server推送的消息。

#### 三. 快速使用

- 工程依赖:
JDK1.7或更高版本 

- 配置proxy server


#### 四. 关于DLQ
<img src="docs/static_files/DLQ.jpg"/>

- proxy-server下推消息后，如果3s后未收到ack，将重复投递，共投递10次。10次后，将投递失败的消息写入kafka的主题为<topic>-dlq中。
- 将失败消息的msgId和写入kafka的dlq中的offset的映射关系写入zk，后续可通过msgId查看DLQ消息。
