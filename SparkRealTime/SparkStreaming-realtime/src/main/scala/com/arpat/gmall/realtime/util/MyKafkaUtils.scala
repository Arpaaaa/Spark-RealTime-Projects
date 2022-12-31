package com.arpat.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.codehaus.jackson.map.deser.std.StringDeserializer

import java.util
import scala.collection.mutable


/**
 * Kafka工具类，用于生产和消费数据
 */
object MyKafkaUtils {

    /**
     * 消费者配置
     */
    private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
        //kafka 集群位置
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
        //kv反序列化
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> MyConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        //groupId

        //offset提交
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> MyConfig.ENABLE_AUTO_COMMIT_CONFIG,
        //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG 自动提交时间间隔
        //offset重置
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> MyConfig.AUTO_OFFSET_RESET_CONFIG
    )


    /**
     * 基于spark streaming 消费，获取到Kafka DStream
     */
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        //将kafka配置参数的groupId进行替换
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

        val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
        //最后返回KafkaDStream
        KafkaDStream
    }

    /**
     * 基于sparlStreaming,按照指定的offset进行消费
     *
     * @param ssc
     * @param topic
     * @param groupId
     * @return
     */
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
        //将kafka配置参数的groupId进行替换
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsets)

        val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
        //最后返回KafkaDStream
        KafkaDStream
    }

    /**
     * 生产者对象
     */
    var producer: KafkaProducer[String, String] = createProducer()

    def createProducer(): KafkaProducer[String, String] = {
        val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
        //集群位置
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
        //kv序列化
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MyConfig.KEY_SERIALIZER_CLASS_CONFIG)
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyConfig.VALUE_SERIALIZER_CLASS_CONFIG)

        //acks
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, MyConfig.ACKS_CONFIG)

        producer = new KafkaProducer[String, String](producerConfigs)
        producer
    }

    /**
     * 生产(按照默认粘性分区策略）
     */
    def send(topic: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, msg))
    }

    /**
     * 指定key分区的发送
     *
     * @param topic
     * @param key
     * @param msg
     */
    def send(topic: String, key: String, msg: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, key, msg))
    }

    /**
     * 刷写缓冲区
     */
    def flush(): Unit = {
        if (producer != null) {
            producer.flush()
        }
    }

    /**
     * 关闭生产者对象
     */
    def close(): Unit = {
        if (producer != null) producer.close()
    }

}
