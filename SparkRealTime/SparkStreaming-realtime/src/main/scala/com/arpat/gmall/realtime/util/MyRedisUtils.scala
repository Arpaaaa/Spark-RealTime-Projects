package com.arpat.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis 工具类
 * 用于获取Jedis连接
 */

object MyRedisUtils {

    var jedisPool:JedisPool = null;

    def getJedisClient(): Jedis ={
        if (jedisPool == null){

            //创建连接池对象
            val jedisPoolConfig = new JedisPoolConfig()
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setMaxIdle(20);
            jedisPoolConfig.setMinIdle(20);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            jedisPoolConfig.setTestOnBorrow(true);

            val host: String = MyPropsUtils(MyConfig.REDIS_HOST)
            val port: String = MyPropsUtils(MyConfig.REDIS_PORT)

            jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)

        }
        jedisPool.getResource
    }

}
