package com.atguigu.gmall0105.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDbMaxwell {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_maxwell_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic="GMALL0105_DB_M"
    val groupId="base_db_maxwel_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]]=null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc)
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
        println(jsonObj.toString)
        if(jsonObj.getJSONObject("data")!=null && !jsonObj.getJSONObject("data").isEmpty
          && !"delete".equals(jsonObj.getString("type")) &&
          (("order_info".equals(jsonObj.getString("table"))&&"insert".equals(jsonObj.getString("type") ))
          ||"order_detail".equals(jsonObj.getString("table"))
          ||"base_province".equals(jsonObj.getString("table"))
            ||"user_info".equals(jsonObj.getString("table"))
            ||"base_category3".equals(jsonObj.getString("table"))
            ||"base_trademark".equals(jsonObj.getString("table"))
            ||"spu_info".equals(jsonObj.getString("table"))
            ||"sku_info".equals(jsonObj.getString("table"))
          )
        ){
          val jsonString=jsonObj.getString("data")
          val tableName: String = jsonObj.getString("table")
          val topic="ODS_"+tableName.toUpperCase
       //   Thread.sleep(500)
          MyKafkaSink.send(topic,jsonString)   //非幂等的操作 可能会导致数据重复
        }
      }

      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()
    
    
    




  }



}
