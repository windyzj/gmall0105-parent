package com.atguigu.gmall0105.publisher.service.impl;

import com.atguigu.gmall0105.publisher.mapper.TrademarkStatMapper;
import com.atguigu.gmall0105.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MySQLServiceImpl implements MySQLService {


    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademarkStat(String startTime, String endTime, int topn) {
        return trademarkStatMapper.selectTrademarkSum(  startTime,   endTime,   topn);
    }
}
