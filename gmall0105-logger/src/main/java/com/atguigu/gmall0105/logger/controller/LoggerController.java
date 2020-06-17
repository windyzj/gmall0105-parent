package com.atguigu.gmall0105.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


//@RestController  //@RestController = @Controller + @ResponseBody
@RestController
@Slf4j
public class LoggerController {
   // @ResponseBody  决定方法的返回值是 网页 还是 文本
    //@Slf4j 会帮你补充log的声明 和 实现 的 代码

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String  applog(@RequestBody  String logString  ){
        //1 输出日志
        System.out.println(logString);
        log.info(logString);

        // 分流
        JSONObject jsonObject = JSON.parseObject(logString);
        if(jsonObject.getString("start")!=null&&jsonObject.getString("start").length()>0){
            //启动日志
            kafkaTemplate.send("GMALL_STARTUP_0105",logString);
        }else{
            //事件日志
            kafkaTemplate.send("GMALL_EVENT_0105",logString);
        }

        return logString;
    }




}
