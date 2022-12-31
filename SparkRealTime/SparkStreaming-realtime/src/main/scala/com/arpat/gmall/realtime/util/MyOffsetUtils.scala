package com.arpat.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable


/**
 * offset工具类 用于往redis中存储和读取offset
 * 管理方案
 *  1. 后置提交偏移量 -> 手动控制偏移量提交
 *     2. 手动控制偏移量提交 -> SparkStreaming提供了手动提交方案，但不使用，需要对DStream的结构进行转化
 *     3. 手动的提取偏移量维护到redis中
 *     -> 从kafka中消费数据，先提取offset
 *     -> 等数据成功写出后，将offset保存到redis中
 *     -> 在kafka消费数据之前，到redis中读取offset
 *     4. 手动将offset存储到redis中，每次消费数据需要redis中存储的offset，消费结束后保存offset到redis中
 */
object MyOffsetUtils {

    /**
     * 往Redis中存储Offset
     * 问题：存的offset从哪来-从消费的数据中提取
     * offset的结构是什么？-> kafka中的offset维护的结构,按照kv结构进行维护
     * groupId + topic + partition => offset
     * “gtp"是key，offset是value
     * 在Redis中怎么存？
     * 类型怎么选：hash
     * key: groupId + topic
     * value: partition - offset
     * 写入API: hset / hmset
     * 读取API: hgetall
     * 是否过期：不过期
     */

    /**
     * 存储offset方法
     * @param topic
     * @param groupId
     * @param offsetRanges
     */
    def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {

        if(offsetRanges !=null && offsetRanges.length >0 ){
            //定义一个offsets HashMap用来存储offset信息
            val offsets: util.HashMap[String, String] = new util.HashMap[String, String]()
            for (offsetRange <- offsetRanges) {
                val partition: Int = offsetRange.partition
                val endOffset: Long = offsetRange.untilOffset
                offsets.put(partition.toString,endOffset.toString)
            }
            println("submit offsets:" + offsets)
            val redisKey:String = s"offsets:$topic:$groupId"

            //往redis存储
            //获取Jedis连接
            val jedis: Jedis = MyRedisUtils.getJedisClient()
            jedis.hset(redisKey,offsets)

            jedis.close()
        }
    }


    /**
     * 从Redis中获取offset
     * 如何让sparkStreaming 通过指定的offset进行消费
     * sparkStreaming要求的offset格式是什么
     *  util.Map[TopicPartition, Long(offset)]
     */

    def readOffset(topic:String,groupId:String): Map[TopicPartition,Long] ={
        val jedis: Jedis = MyRedisUtils.getJedisClient()
        val redisKey: String = s"offsets:$topic:$groupId"
        val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)

        println("Read offsets: " + offsets)
        val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

        //将Java 的map转换为scala中的map进行迭代
        import scala.collection.JavaConverters._
        for ((partition,offset) <- offsets.asScala) {
            val tp: TopicPartition = new TopicPartition(topic, partition.toInt)
            results.put(tp,offset.toLong)

        }
        jedis.close()
        results.toMap
    }






}
