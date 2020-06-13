package com.atguigu.gmall0105.logger.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.atguigu.gmall0105.logger.controller")
public class Gmall0105LoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0105LoggerApplication.class, args);
    }

}
