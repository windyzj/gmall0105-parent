package com.atguigu.gmall0105.realtime.otherDoubleStreamJoin

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
  def main(args: Array[String]): Unit = {
     val sparkConf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val orderInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream( "T_ORDER_INFO",ssc)
    val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("T_ORDER_DETAIL",ssc)

    val orderInfoDstream: DStream[OrderInfo] = orderInputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //补充 日期字段
      //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = datetimeArr(0)
      orderInfo.create_hour = datetimeArr(1).split(":")(0)
    //  val teltuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
    //  orderInfo.consignee_tel = teltuple._1 + "****" + teltuple._2.splitAt(4)._2 //138****0101
      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map { record =>
      val orderDetailJson: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
      orderDetail
    }
    //流和流之间的join
    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id.toString,orderInfo))
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id.toString, orderDetail))
    val orderJoinedDstream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)

    val orderFullJoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>


      //1主表部分
      val saleDetailList = new ListBuffer[SaleDetail]
      val jedis: Jedis = RedisUtil.getJedisClient
      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        //1.1 在同一批次能够关联， 两个对象组合成一个新的宽表对象
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }
        //1.2  转换成json写入缓存
        val orderInfoJson: String = JSON.toJSONString(orderInfo,new SerializeConfig(true))
        // redis写入   type ? string        key ?    order_info:[order_id]        value ?   orderInfoJson   ex? 600s
        // 为什么不用集合 比如hash 来存储整个的orderInfo 清单呢
        //1 没必要 因为不需要一下取出整个清单
        //2 超大hash 不容易进行分布式
        // 3  hash 中的单独k-v  没法设定过期时间
        val orderInfokey = "order_info:" + orderInfo.id
        jedis.setex(orderInfokey, 600, orderInfoJson)
        //1.3   查询缓存中是否有对应的orderDetail
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
          import scala.collection.JavaConversions._
          for (orderDetailJsonString <- orderDetailJsonSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        }

      } else { //2  从表
        val orderDetail: OrderDetail = orderDetailOption.get
        //2.1 转换成json写入缓存
        val orderDetailJson: String = JSON.toJSONString(orderDetail,new SerializeConfig(true))
        // Redis ？  type ?   set     key ?   order_detail:[order_id]       value ? orderDetailJsons
        //从表如何存储到redis?
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)

        //2.2  从表查询缓存中主表信息
        val orderInfokey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfokey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          saleDetailList += new SaleDetail(orderInfo, orderDetail)
        }

      }
      jedis.close()
      saleDetailList


    }


   // saleDetailDstream.print(100)






 //   saleDetailWithUserDstream.print(100)

    saleDetailDstream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailItr=>
        val saleDetailList: List[(String, SaleDetail)] = saleDetailItr.toList.map(saleDetail=>(saleDetail.order_detail_id,saleDetail))
        MyEsUtil.bulkDoc(saleDetailList,"")

      }

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
