package com.atguigu.gmall0105.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall0105.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderDetailApp {



    def main(args: Array[String]): Unit = {

      //加载流
      val sparkConf: SparkConf = new SparkConf().setAppName("order_detail_app").setMaster("local[4]")
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      val topic = "ODS_ORDER_DETAIL"
      val groupId = "order_detail_group"
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
      val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
        val jsonString: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
      }

     // 合并维表数据
    // 品牌 分类 spu  作业
    //  orderDetailDstream.
      /////////////// 合并 商品信息////////////////////

      val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailItr =>
        val orderDetailList: List[OrderDetail] = orderDetailItr.toList
        if(orderDetailList.size>0) {
          val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
          val sql = "select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall0105_sku_info  where id in ('" + skuIdList.mkString("','") + "')"
          val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
          val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
          for (orderDetail <- orderDetailList) {
            val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
            orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
            orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
            orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
            orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
            orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
            orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
          }
        }
        orderDetailList.toIterator
      }





      orderDetailWithSkuDstream.foreachRDD{rdd=>
          rdd.foreach{ orderDetail=>
            val orderDetailJsonString: String  = JSON.toJSONString(orderDetail,new SerializeConfig(true))
             MyKafkaSink.send("DWD_ORDER_DETAIL",orderDetail.order_id.toString,orderDetailJsonString)
          }

        OffsetManager.saveOffset(topic,groupId,offsetRanges)
      }

      ssc.start()
      ssc.awaitTermination()
    }

}
