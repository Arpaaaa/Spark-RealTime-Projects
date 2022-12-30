package com.arpat.gmall.realtime.util

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object MyConfig {

    val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap.server"

    //producer
    val KEY_SERIALIZER_CLASS_CONFIG: Class[StringSerializer] = classOf[StringSerializer]
    val VALUE_SERIALIZER_CLASS_CONFIG: Class[StringSerializer] = classOf[StringSerializer]
    val ACKS_CONFIG: String = "all"


    //consumer
    val KEY_DESERIALIZER_CLASS_CONFIG: Class[StringDeserializer] = classOf[StringDeserializer]
    val VALUE_DESERIALIZER_CLASS_CONFIG: Class[StringDeserializer] = classOf[StringDeserializer]
    val ENABLE_AUTO_COMMIT_CONFIG: String = "true"
    val AUTO_OFFSET_RESET_CONFIG:String = "latest"
}
