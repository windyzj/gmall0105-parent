package com.atguigu.gmall0105.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

object OrderInfoApp {

  def main(args: Array[String]): Unit = {

      //加载流
      val sparkConf: SparkConf = new SparkConf().setAppName("order_info_app").setMaster("local[4]")
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      val topic = "ODS_ORDER_INFO"
      val groupId = "order_info_group"
      val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
      var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
      if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
        recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
      } else {
        recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
      }

      //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
      var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
      val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver? executor?  //周期性的执行
        rdd
      }

      //基本的结构转换 ，补时间字段
      val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = createTimeArr(0)
        val timeArr: Array[String] = createTimeArr(1).split(":")
        orderInfo.create_hour = timeArr(0)
        orderInfo
      }

      //查询hbase中用户状态
    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      //每分区的操作
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if(orderInfoList.size>0){
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        val sql = "select user_id , if_consumed from user_state0105 where user_id in ('" + userIdList.mkString("','") + "')"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userStateMap: Map[String, String] = userStateList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
        for (orderInfo <- orderInfoList) { //每条数据
          //得到是否消费
          val if_consumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
          if (if_consumed != null || if_consumed == "1") { //如果是消费用户  首单标志置为0
            orderInfo.if_first_order = "0";
          } else {
            orderInfo.if_first_order = "1";
          }
        }
      }
      orderInfoList.toIterator
    }

    orderInfoWithFirstFlagDstream.print(1000)

      /*  查询次数太多 欠优化
        orderInfoDstream.map{orderInfo=>
        val sql="select user_id , if_consumed from user_state0105 where user_id='"+orderInfo.user_id+"'"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        if(userStateList!=null&&userStateList.size>0){
          val userStateJsonObj: JSONObject = userStateList(0)
          if(userStateJsonObj.getString("IF_CONSUMED").equals("1")){
            orderInfo.if_first_order="0";
          }else{
            orderInfo.if_first_order="1";
          }
        }else{
          orderInfo.if_first_order="1";
        }
        orderInfo
      }*/



      // 通过用户状态为订单增加 首单标志


      // 维度数据的合并

      // 保存 用户状态--> 更新hbase 维护状态
    orderInfoWithFirstFlagDstream.foreachRDD{rdd=>
    //Seq 中的字段顺序 和 rdd中对象的顺序一直
      // 把首单的订单 更新到用户状态中
      val newConsumedUserRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderInfo=> UserState(orderInfo.user_id.toString,"1" ))
      newConsumedUserRDD.saveToPhoenix("USER_STATE0105",Seq("USER_ID","IF_CONSUMED"),
        new Configuration,Some("hdp1,hdp2,hdp3:2181"))


      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

      // 保存 订单明细 --> OLAP  即席查询
      // 保存到 Kafka --> 继续加工


      ssc.start()
      ssc.awaitTermination()
    }


}
