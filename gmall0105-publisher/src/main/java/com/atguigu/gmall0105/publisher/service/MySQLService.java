package com.atguigu.gmall0105.publisher.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    //1 省市地区
    //2  品牌
    public List<Map> getTrademarkStat(String startTime , String endTime , int topn);

    //3 年龄段
    // 4 性别
    // 5 spu
    // 6 品类

}
