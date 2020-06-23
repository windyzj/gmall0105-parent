package com.atguigu.gmall0105.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseDbCanal {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic="GMALL0105_DB_C"
    val groupId="base_db_canal_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]]=null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var  offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val  inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  //driver? executor?  //周期性的执行
      rdd
    }


    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }


    jsonObjDstream.foreachRDD{rdd=>
      // 推回kafka
      rdd.foreach{jsonObj=>
        if(jsonObj.getString("type").equals("INSERT")||jsonObj.getString("type").equals("UPDATE")||jsonObj.getString("type").equals("DELETE")){
          val jsonArr=jsonObj.getJSONArray("data")
          val tableName: String = jsonObj.getString("table")
          val topic="ODS_"+tableName.toUpperCase
          import  scala.collection.JavaConversions._
          for (jsonObj <- jsonArr ) {
            val msg: String = jsonObj.toString
            MyKafkaSink.send(topic,msg)   //非幂等的操作 可能会导致数据重复
          }
        }

      }

      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
    
    
    




  }



}
