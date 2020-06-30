package com.atguigu.gmall0105.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105.publisher.service.ClickHouseService;
import com.atguigu.gmall0105.publisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    EsService esService;

    @Autowired
    ClickHouseService clickHouseService;

    @RequestMapping(value = "realtime-total",method = RequestMethod.GET)
    public String realtimeTotal(@RequestParam("date") String dt){
        List<Map<String,Object>>  rsList=new ArrayList<>();

        Map<String,Object> dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal=0L;
        try {
            dauTotal = esService.getDauTotal(dt);
        }catch ( Exception e){
            e.printStackTrace();
        }
        if(dauTotal!=null){
            dauMap.put("value",dauTotal);
        }else {
            dauMap.put("value",0L);
        }

        rsList.add(dauMap);

        Map<String,Object> newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        rsList.add(newMidMap);

        Map<String,Object> orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        BigDecimal orderAmount = clickHouseService.getOrderAmount(dt);
        orderAmountMap.put("value",orderAmount);
        rsList.add(orderAmountMap);

        return  JSON.toJSONString(rsList);
    }

    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dt){
        if(id.equals("dau")){
            Map dauHourMapTD = esService.getDauHour(dt);
            String yd = getYd(dt);
            Map dauHourMapYD = esService.getDauHour(yd);

            Map<String,Map<String,Long>> rsMap=new HashMap<>();
            rsMap.put("yesterday",dauHourMapYD);
            rsMap.put("today",dauHourMapTD);
            return  JSON.toJSONString(rsMap);
        }else if(id.equals("order_amount")){
            Map orderAmountHourMapTD = clickHouseService.getOrderAmountHour(dt);
            String yd = getYd(dt);
            Map orderAmountHourMapYD = clickHouseService.getOrderAmountHour(yd);

            Map<String,Map<String,BigDecimal>> rsMap=new HashMap<>();
            rsMap.put("yesterday",orderAmountHourMapYD);
            rsMap.put("today",orderAmountHourMapTD);
            return  JSON.toJSONString(rsMap);
        }else{
            return  null;
        }

    }

    private  String getYd(String today){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date todayDate = dateFormat.parse(today);
            Date ydDate = DateUtils.addDays(todayDate, -1);
            return   dateFormat.format(ydDate);

        } catch (ParseException e) {
            e.printStackTrace();
            throw  new RuntimeException("日期格式不正确");
        }

    }



}
