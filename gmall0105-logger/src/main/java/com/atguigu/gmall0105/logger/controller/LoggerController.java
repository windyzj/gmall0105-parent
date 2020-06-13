package com.atguigu.gmall0105.logger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


//@RestController  //@RestController = @Controller + @ResponseBody
@RestController
public class LoggerController {
   // @ResponseBody  决定方法的返回值是 网页 还是 文本

    @RequestMapping("/applog")
    public String  log(@RequestBody  String log  ){
        System.out.println(log);
        return log;
    }


}
