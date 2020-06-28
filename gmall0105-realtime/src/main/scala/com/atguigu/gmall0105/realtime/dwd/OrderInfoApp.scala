package com.atguigu.gmall0105.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.dim.{ProvinceInfo, UserState}
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
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
          if (if_consumed != null && if_consumed == "1") { //如果是消费用户  首单标志置为0
            orderInfo.if_first_order = "0";
          } else {
            orderInfo.if_first_order = "1";
          }
        }
      }
      orderInfoList.toIterator
    }

    // 利用hbase  进行查询过滤 识别首单，只能进行跨批次的判断
    //  如果新用户在同一批次 多次下单 会造成 该批次该用户所有订单都识别为首单
    //  应该同一批次一个用户只有最早的订单 为首单 其他的单据为非首单
    // 处理办法： 1 同一批次 同一用户  2 最早的订单  3 标记首单
    //           1 分组： 按用户      2  排序  取最早  3 如果最早的订单被标记为首单，除最早的单据一律改为非首单
    //           1  groupbykey       2  sortWith    3  if ...

    //调整结果变为k-v 为分组做准备
    val OrderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDstream.map(orderInfo=>(orderInfo.user_id,orderInfo))
   //分组 按用户
    val orderInfoGroupByUidDstream: DStream[(Long, Iterable[OrderInfo])] = OrderInfoWithKeyDstream.groupByKey()
   //组内进行排序 ，最早的订单被标记为首单，除最早的单据一律改为非首单
    val orderInfoWithFirstRealFlagDstream: DStream[OrderInfo] = orderInfoGroupByUidDstream.flatMap { case (userId, orderInfoItr) =>
      if (orderInfoItr.size > 1) {
        //排序
        val userOrderInfoSortedList: List[OrderInfo] = orderInfoItr.toList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        val orderInfoFirst: OrderInfo = userOrderInfoSortedList(0)
        if (orderInfoFirst.if_first_order == "1") {
          for (i <- 1 to userOrderInfoSortedList.size - 1) {
            val orderInfoNotFirst: OrderInfo = userOrderInfoSortedList(i)
            orderInfoNotFirst.if_first_order = "0"
          }
        }
        userOrderInfoSortedList
      } else {
        orderInfoItr.toList
      }
    }

    // 优化 ： 因为传输量小  使用数据的占比大  可以考虑使用广播变量     查询hbase的次数会变小   分区越多效果越明显
    //利用driver进行查询 再利用广播变量进行分发
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoWithFirstRealFlagDstream.transform { rdd =>
      //driver  按批次周期性执行
      //driver中查询
      val sql = "select  id,name,area_code,iso_code,iso_3166_2 from gmall0105_province_info "
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      //封装广播变量
      val provinceMap: Map[String, ProvinceInfo] = provinceInfoList.map { jsonObj =>
        val provinceInfo = ProvinceInfo(jsonObj.getString("ID"),
          jsonObj.getString("NAME"),
          jsonObj.getString("AREA_CODE"),
          jsonObj.getString("ISO_CODE"),
          jsonObj.getString("ISO_3166_2")
        )
        (provinceInfo.id, provinceInfo)
      }.toMap

      val provinceBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo => //  ex 2
        val provinceMap: Map[String, ProvinceInfo] = provinceBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        if(provinceInfo!=null){
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
          orderInfo.province_iso_3166_2 = provinceInfo.iso_3166_2
        }
        orderInfo
      }
      orderInfoWithProvinceRDD

    }

    //////////////////用户信息关联//////////////////////////
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if(orderList.size>0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall0105_user_info where id in ('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
        }
      }
      orderList.toIterator
    }









    //    orderInfoWithFirstRealFlagDstream.mapPartitions{orderInfoItr=>
//      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
//
//      val provinceList: List[JSONObject] = provinceBC.value
//      null
//    }





//待优化 可以节省查询次数
/*    orderInfoWithFirstRealFlagDstream.mapPartitions{orderInfoItr=>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      val provinceIdList: List[Long] = orderInfoList.map(_.province_id)

      val sql = "select  id,name,area_code,iso_code from gmall0105_province_info where  id in ('" + provinceIdList.mkString("','") + "')"
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      null
    }*/



    orderInfoWithUserDstream.print(1000)

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
    orderInfoWithUserDstream.cache()
      // 保存 用户状态--> 更新hbase 维护状态
    orderInfoWithUserDstream.foreachRDD{rdd=>
      //driver
    //Seq 中的字段顺序 和 rdd中对象的顺序一直
      // 把首单的订单 更新到用户状态中
      val newConsumedUserRDD: RDD[UserState] = rdd.filter(_.if_first_order=="1").map(orderInfo=> UserState(orderInfo.user_id.toString,"1" ))
      newConsumedUserRDD.saveToPhoenix("USER_STATE0105",Seq("USER_ID","IF_CONSUMED"),
        new Configuration,Some("hdp1,hdp2,hdp3:2181"))



    }

    orderInfoWithUserDstream.foreachRDD{rdd=>
        rdd.foreachPartition{ orderInfoItr=>
          val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          val orderInfoWithIdList: List[(String, OrderInfo)] = orderInfoList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
          val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      //    MyEsUtil.bulkDoc(orderInfoWithIdList,"gmall0105_order_info_"+dateString)
          for (orderInfo <- orderInfoList ) {
            val orderInfoJsonString: String  = JSON.toJSONString(orderInfo,new SerializeConfig(true))
             MyKafkaSink.send("DWD_ORDER_INFO",orderInfo.id.toString ,  orderInfoJsonString)
          }

        }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }



      // 保存 订单明细 --> OLAP  即席查询
      // 保存到 Kafka --> 继续加工


      ssc.start()
      ssc.awaitTermination()
    }


}
