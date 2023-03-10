# Spark-RealTime-Projects
Real-Time Analysis Using Spark 

## Chapter2 日志数据采集和分流

整体架构：

 日志文件 -> Flume -> Kafka统一Topic -> SparkStreaming -> Kafka 不同的topic

Kafka最终的分流topic有：

* 启动日志
* 页面访问
* 动作日志
* 曝光日志
* 错误日志



整体逻辑：日志生成器生成日志数据到Kafka中，我们编写程序通过sparkStreaming将Kafka中的日志数据消费，并将最终的结果写回到不同的Kafka主题中。


## 代码优化 —— 精确一次消费

1. 至少一次消费
   主要是保证数据不会丢失，但存在数据重复问题

2. 最多一次消费
   主要是保证数据不会重复，但存在数据丢失问题

3. 精确一次消费
   消息一定会被处理并只被处理一次

### 消费问题

1. 漏消费问题-丢失数据

    Kafka先提交了偏移量offset，后写出数据。在此过程中写出数据失败，数据不会重发导致漏消费问题。

2. 重复消费-重复计算

    Kafka先写出了数据，后提交偏移量offset。在提交offset时失败，导致重复消费数据。

### 解决方法

1. 将写出数据和提交offset做成事务，进行原子绑定，达到原子性操作。
    
    利用关系型数据库的事务进行处理，一般在数据量足够少的情况下使用。在数据量大的情况下需要考虑分布式事务的问题，会有很大的复杂性

2. 后置提交offset+幂等性

    手动提交偏移量 + 幂等性处理。幂等性处理就是数据提交一次和提交多次的效果是相同的。

    一般实际生产中通常会利用zookeeper,Redis,Mysql等工具进行手动保存对偏移量offset进行保存。

## 代码优化 -Kafka消息发送问题

Kafka消息的发送分为同步发送和异步发送，kafka默认使用异步发送的方式，在发送消息时，首先将消息发送到缓冲区中，等到缓冲区写满或者达到指定的时间时才会
真正的将缓冲区中的数据写到Broker。我们通常在消息发送到缓冲区时认为数据发送成功并将offset手动提交，如果此时发生故障导致数据无法写到broker而offset
已经提交，那么这部分数据就会遗漏。

### 解决方法

Kafka的生产者对象提供了flush方法，可以强制将缓冲区的数据刷到Broker

使用foreachPartition进行分区操作，并在每个分区数据发送后flush缓冲区