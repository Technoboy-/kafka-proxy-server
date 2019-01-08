#### 一. 背景
Kafka的consumer数大于partition数时，多出的consumer将无法消费到消息。如果想降低消息消费的延迟时间，只能扩大partion数量，
进而增加consumer的数量，来增加消费能力，解决消息延迟性。有些情形下，少量的partiton足以满足上游生产消息的速率，consumer的消费能力却成为瓶颈，
因为consumer可能要处理更多的业务逻辑。因此，一种代理模式就出现了，即kafka-push-server。

#### 二. 设计图

<img src="docs/static_files/push-server.png"/>


