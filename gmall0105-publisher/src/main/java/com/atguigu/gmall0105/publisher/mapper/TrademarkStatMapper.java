package com.atguigu.gmall0105.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface TrademarkStatMapper {


    public List<Map> selectTrademarkSum(@Param("startTime") String startTime , @Param("endTime")String endTime ,@Param("topn") int topn);
}
