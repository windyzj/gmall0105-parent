package com.atguigu.gmall0105.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105.publisher.CustomerInfo;
import com.atguigu.gmall0105.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("trademarkStat")
    public String trademarkStat(@RequestParam("startTime") String startTime,@RequestParam("endTime") String endTime, @RequestParam("topn") int topn){
        List<Map> mapList = mySQLService.getTrademarkStat(startTime, endTime, topn);
        List<Map> datavFormatList=new ArrayList<>();
        for (Map map : mapList) {
            Map dataVmap=new HashMap();
            dataVmap.put("x",map.get("trademark_name"));
            dataVmap.put("y",map.get("order_amount"));
            dataVmap.put("s","1");
            datavFormatList.add(dataVmap);
        }


        return JSON.toJSONString(datavFormatList);

    }
}
