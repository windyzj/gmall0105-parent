package com.atguigu.gmall0105.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis



object OffsetManager {


  //从redis中读取偏移量
  def  getOffset(topicName:String,groupId:String): Map[TopicPartition,Long]  ={
    // Redis 中偏移量的保存格式   type?  hash   key ?  "offset:[topic]:[groupid]"    field ?  partition_id  value ?  offset     expire
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    import scala.collection.JavaConversions._
    val kafkaOffsetMap:  Map[TopicPartition, Long] = offsetMap.map { case (patitionId, offset) =>
      (new TopicPartition(topicName, patitionId.toInt), offset.toLong)
    }.toMap
    kafkaOffsetMap

  }



  //把偏移量写入redis

}
