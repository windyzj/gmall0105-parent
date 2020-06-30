package com.atguigu.gmall0105.publisher.service.impl;

import com.atguigu.gmall0105.publisher.mapper.OrderWideMapper;
import com.atguigu.gmall0105.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {
        return orderWideMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //进行转换
//        List<Map>     [{"hr":10,"order_amount":1000.00} ,{"hr":11,"order_amount":2000.00}...... ]
//        Map    {"10":1000.00,"11":2000.00,...... }
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        Map<String,BigDecimal> hourMap=new HashMap<>();
        for (Map map : mapList) {
            hourMap.put(  String.valueOf(map.get("hr"))  , (BigDecimal) map.get("order_amount"));
        }
        return hourMap;
    }
}
