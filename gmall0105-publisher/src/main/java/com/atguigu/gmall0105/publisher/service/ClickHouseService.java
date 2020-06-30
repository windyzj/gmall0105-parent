package com.atguigu.gmall0105.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ClickHouseService {

    public BigDecimal getOrderAmount(String date);

    public Map getOrderAmountHour(String date);


}
