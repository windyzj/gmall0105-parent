package com.atguigu.gmall0105.publisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {

    //查询当日总额
    public BigDecimal selectOrderAmount(String date);

    //查询当日分时交易额
    public List<Map> selectOrderAmountHour(String date);
}
