package com.atguigu.gmall0105.realtime.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {
    //加载流
    val sparkConf: SparkConf = new SparkConf().setAppName("order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicOrderInfo = "DWD_ORDER_INFO"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderInfo = "dws_order_info_group"
    val groupIdOrderDetail = "dws_order_detail_group"

    // 订单主表
    val orderInfokafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdOrderInfo)
    var orderInfoRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfokafkaOffsetMap != null && orderInfokafkaOffsetMap.size > 0) {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, orderInfokafkaOffsetMap, groupIdOrderInfo)
    } else {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupIdOrderInfo)
    }

    // 订单明细
    val orderDetailkafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupIdOrderDetail)
    var orderDetailRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailkafkaOffsetMap != null && orderDetailkafkaOffsetMap.size > 0) {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, orderDetailkafkaOffsetMap, groupIdOrderDetail)
    } else {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupIdOrderDetail)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputStream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputStream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    ///////////////////////////////////
    /////////////结构调整//////////////
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    }
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }


    orderInfoDstream.print(1000)
    orderDetailDstream.print(1000)

    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    //无法保证应对配对的主表和从表数据都在一个批次中，join有可能丢失数据
    // val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)


    // 1 开窗口
    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(10),Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(10),Seconds(5))

    // 2  join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)
    // 3 去重
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()
      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
        //Redis  type? set  key order_join_keys   value    orderDetail.id
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }
      }
      orderJoinedNewList.toIterator
    }
    orderJoinedNewDstream


    ssc.start()
    ssc.awaitTermination()
  }

}
