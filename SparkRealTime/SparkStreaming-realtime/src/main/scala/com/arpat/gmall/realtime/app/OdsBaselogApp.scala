package com.arpat.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.arpat.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.arpat.gmall.realtime.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消息分流
 * 1. 准备实时处理环境 streamingContext
 * 2. 从Kafka中消费数据
 * 3. 处理数据
 * 3.1 转换数据结构
 * 专用结构 Bean
 * 通用结构 Map，List等通用结构
 * 3.2 分流
 * 4. 写出到DWD层
 */
object OdsBaselogApp {
    def main(args: Array[String]): Unit = {
        //1 准备实时处理环境
        //注意spark执行的并行度要和Kafka的分区个数相匹配
        val sparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        //2. 从kafka中消费数据
        val topicName: String = "ODS_BASE_LOG_1018" //对应生成器配置中的主题名
        val groupId: String = "ODS_BASE_LOG_GROUP_1018"

        //TODO:从Redis中获取offset，按照指定offset进行消费
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)

        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsets != null && offsets.nonEmpty) {
            //指定的offset进行消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
        } else {
            //按照默认的offset进行消费
            kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
        }


        //TODO:从数据中提取offsets,不对流中的数据进行处理
        var offsetRanges: Array[OffsetRange] = null;
        val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //在Driver端执行
                rdd
            }
        )

        //3.处理数据
        //3.1 转换数据结构
        val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(
            consumerRecord => {
                //获取record中的value，value就是日志数据
                val log: String = consumerRecord.value()
                //转换成json对象
                val jSONObject: JSONObject = JSON.parseObject(log)
                //返回json对象
                jSONObject
            }
        )
        //测试：流中数据的打印 jsonObjDStream.print(50)
        //3.2 分流
        /* 日志数据主要分为两种：
        *   页面访问数据
        *       公共字段
        *       页面数据
        *       曝光数据
        *       事件数据
        *       错误数据
        *   启动数据
        *       公共字段
        *       启动数据*/

        val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" //页面访问
        val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
        val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC" //页面事件
        val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" //启动数据
        val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" //错误数据

        //分流规则
        //错误数据：不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
        //页面数据：拆分成页面访问、曝光、事件分别发送到对应topic
        //启动数据：发送到对应topic

        jsonObjDStream.foreachRDD(
            rdd => {
                rdd.foreach(
                    jsonObj => {
                        //分流过程
                        val errObject: JSONObject = jsonObj.getJSONObject("err")
                        if (errObject != null) {
                            //将错误数据发送到DWD_ERROR_LOG_TOPIC
                            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
                        } else {
                            //提取公共字段
                            val commonObject: JSONObject = jsonObj.getJSONObject("common")
                            val ar: String = commonObject.getString("ar")
                            val uid = commonObject.getString("uid")
                            val os = commonObject.getString("os")
                            val ch = commonObject.getString("ch")
                            val is_new = commonObject.getString("is_new")
                            val md = commonObject.getString("md")
                            val mid = commonObject.getString("mid")
                            val vc = commonObject.getString("vc")
                            val ba = commonObject.getString("ba")

                            //提取时间戳
                            val ts: Long = jsonObj.getLong("ts")

                            //提取页面数据
                            val pageObject: JSONObject = jsonObj.getJSONObject("page")
                            if (pageObject != null) {
                                //提取page字段
                                val pageId = pageObject.getString("page_id")
                                val pageItem = pageObject.getString("item")
                                val pageItemType = pageObject.getString("item_type")
                                val duringTime: Long = pageObject.getLong("during_time")
                                val lastPageId = pageObject.getString("last_page_id")
                                val sourceType = pageObject.getString("source_type")

                                //封装成pageLog对象
                                val pageLog: PageLog = PageLog(mid, uid, ar, ch, is_new, md, ba, os, vc, pageId, lastPageId, pageItem
                                    , pageItemType, duringTime, sourceType, ts)

                                //发送到DWD_PAGE_LOG_TOPIC主题中
                                //new SerializeConfig(true) 无需使用bean对象的getset方法
                                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                                //提取曝光数据
                                val displayJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                                if (displayJsonArr != null && displayJsonArr.size() > 0) {
                                    for (i <- 0 until displayJsonArr.size()) {
                                        //循环拿到每个曝光数据
                                        val displayObj = displayJsonArr.getJSONObject(i)
                                        val displayType: String = displayObj.getString("display_type")
                                        val displayItem: String = displayObj.getString("item")
                                        val displayItemType: String = displayObj.getString("item_type")
                                        val posId: String = displayObj.getString("pos_id")
                                        val order: String = displayObj.getString("order")

                                        //写到DWD_PAGE_DISPLAY_TOPIC
                                        val pageDisplayLog = PageDisplayLog(mid, uid, ar, ch, is_new, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime
                                            , sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                                        MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))

                                    }
                                }

                                //提取事件数据
                                val actionJsonArr = jsonObj.getJSONArray("actions")
                                if (actionJsonArr != null && actionJsonArr.size() > 0) {
                                    for (i <- 0 until actionJsonArr.size()) {
                                        //循环拿到每个action数据
                                        val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                                        val actionItem = actionObj.getString("item")
                                        val actionId = actionObj.getString("action_id")
                                        val actionItemType = actionObj.getString("item_type")
                                        val actionTs = actionObj.getString("ts")

                                        //写到DWD_PAGE_ACTION_TOPIC
                                        val pageActionLog = PageActionLog(mid, uid, ar, ch, is_new, md, ba, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, ts)
                                        MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                                    }
                                }
                            }
                            //提取启动数据
                            val startObject: JSONObject = jsonObj.getJSONObject("start")
                            if (startObject != null) {
                                val startEntry: String = startObject.getString("entry")
                                val loadingTimeMs: Long = startObject.getLong("loading_time")
                                val openAdId: String = startObject.getString("open_ad_id")
                                val openAdSkipMs: Long = startObject.getLong("open_ad_skip_ms")
                                val openAdMs: Long = startObject.getLong("open_ad_ms")
                                val startLog: StartLog = StartLog(mid, uid, ar, ch, is_new, md, ba, os, vc, startEntry, openAdId, loadingTimeMs, openAdMs, openAdSkipMs, ts)

                                //发送Kafka
                                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                            }
                        }
                        //分流结束
                    }
                    //foreach里面提交offset：executor端执行，每条数据执行一次
                )
                //foreachRDD里面，foreach外面提交offset：Driver端执行，一批次执行一次（周期性）
                MyOffsetUtils.saveOffset(topicName,groupId, offsetRanges)
            }
        )
        //foreachRDD外面提交offset：Driver端执行，每次启动程序执行一次

        ssc.start()
        ssc.awaitTermination()
    }

}
