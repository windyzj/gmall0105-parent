package com.atguigu.gmall0105.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis



object OffsetManager {


  //从redis中读取偏移量
  def  getOffset(topicName:String,groupId:String): Map[TopicPartition,Long]  ={
    // Redis 中偏移量的保存格式   type?  hash   key ?  "offset:[topic]:[groupid]"    field ?  partition_id  value ?  offset     expire
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import scala.collection.JavaConversions._
    val kafkaOffsetMap:  Map[TopicPartition, Long] = offsetMap.map { case (patitionId, offset) =>
      println("加载分区偏移量："+patitionId +":"+offset  )
      (new TopicPartition(topicName, patitionId.toInt), offset.toLong)
    }.toMap
    kafkaOffsetMap

  }

    def saveOffset(topicName:String ,groupId:String ,offsetRanges: Array[OffsetRange]): Unit ={
      //redis偏移量的写入
      // Redis 中偏移量的保存格式   type?  hash   key ?  "offset:[topic]:[groupid]"    field ?  partition_id  value ?  offset     expire

      val offsetKey="offset:"+topicName+":"+groupId
      val offsetMap:util.Map[String,String]=new util.HashMap()
      //转换结构 offsetRanges -> offsetMap
      for (offset <- offsetRanges) {
        val partition: Int = offset.partition
        val untilOffset: Long = offset.untilOffset
         offsetMap.put(partition+"",untilOffset+"")
         //println("写入分区："+partition +":"+offset.fromOffset+"-->"+offset.untilOffset)
      }
      //写入redis
      if(offsetMap!=null&&offsetMap.size()>0){
        val jedis: Jedis = RedisUtil.getJedisClient
        jedis.hmset(offsetKey,offsetMap)
        jedis.close()
      }


  }



  //把偏移量写入redis

}
