package com.cn.wavetop.dataone.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author yongz
 * @Date 2019/12/10、18:11
 */
@RestController("/hello")
public class HelloSpringCloud {

    @GetMapping("/{name}")
    public String hello(@PathVariable String name){
        return "hello"+name;
    }

}
