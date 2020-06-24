package com.atguigu.gmall0105.realtime.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
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
//   //    orderJoinedDstream.print(1000)

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
      jedis.close()
      orderJoinedNewList.toIterator
    }
    orderJoinedNewDstream.print(1000)

    val orderDetailWideDStream: DStream[OrderDetailWide] = orderJoinedNewDstream.map{case(orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}
   /////////////////////////////////////////////////////////
   // 计算实付分摊需求
////////////////////////////////////////
    // 思路 ：：
//    每条明细已有        1  原始总金额（original_total_amount） （明细单价和各个数的汇总值）
//    2  实付总金额 (final_total_amount)  原始金额-优惠金额+运费
//    3  购买数量 （sku_num)
//    4  单价      ( order_price)
//
//    求 每条明细的实付分摊金额（按明细消费金额比例拆分）
//
//    1  33.33   40    120
//    2  33.33   40    120
//    3   ？     40    120
//
//    如果 计算是该明细不是最后一笔
//      使用乘除法      实付分摊金额/实付总金额= （数量*单价）/原始总金额
//      调整移项可得  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
//
//    如果  计算时该明细是最后一笔
//      使用减法          实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
//    1 减法公式
//      2 如何判断是最后一笔
//      如果 该条明细 （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
//
//
//    两个合计值 如何处理
//      在依次计算的过程中把  订单的已经计算完的明细的【实付分摊金额】的合计
//    订单的已经计算完的明细的【数量*单价】的合计
//    保存在redis中 key设计
//    type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额

//  伪代码
    //    1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
    //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //    3.1  如果不是最后一笔：
                      // 用乘除计算 ： 实付分摊金额=（数量*单价）*实付总金额 / 原始总金额

    //    3.2 如果是最后一笔
                     // 使用减法 ：   实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    4  进行合计保存
              //  hincr
//              【实付分摊金额】的合计，【数量*单价】的合计
    ssc.start()
    ssc.awaitTermination()
  }

}
