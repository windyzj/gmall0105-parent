package com.atguigu.gmall0105.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic="GMALL_STARTUP_0105"
    val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)
  
 //   recordInputStream.map(_.value()).print()

    val jsonObjDstream: DStream[JSONObject] = recordInputStream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }

    jsonObjDstream
    

    ssc.start()
    ssc.awaitTermination()


  }




}
