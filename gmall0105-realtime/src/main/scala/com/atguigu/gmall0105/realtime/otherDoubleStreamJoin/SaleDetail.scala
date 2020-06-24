package com.atguigu.gmall0105.realtime.otherDoubleStreamJoin

import java.text.SimpleDateFormat
import java.util

import com.atguigu.gmall0105.realtime.bean.{OrderDetail, OrderInfo}


case class SaleDetail(
                       var order_detail_id:String =null,
                       var order_id: String=null,
                       var order_status:String=null,
                       var create_time:String=null,
                       var user_id: String=null,
                       var sku_id: String=null,
                       var user_gender: String=null,
                       var user_age: Int=0,
                       var user_level: String=null,
                       var sku_price: Double=0D,
                       var sku_name: String=null,
                       var dt:String=null)
{
  def this(orderInfo:OrderInfo,orderDetail: OrderDetail) {
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo:OrderInfo): Unit ={
    if(orderInfo!=null){
      this.order_id=orderInfo.id.toString
      this.order_status=orderInfo.order_status
      this.create_time=orderInfo.create_time
      this.dt=orderInfo.create_date
      this.user_id=orderInfo.user_id.toString
    }
  }


  def mergeOrderDetail(orderDetail: OrderDetail): Unit ={
    if(orderDetail!=null){
      this.order_detail_id=orderDetail.id.toString
      this.sku_id=orderDetail.sku_id.toString
      this.sku_name=orderDetail.sku_name
      this.sku_price=orderDetail.order_price.toDouble


    }
  }


}

