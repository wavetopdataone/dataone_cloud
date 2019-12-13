package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.service.UserLogService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/userlog")
public class UserLogController {

    @Autowired
    private UserLogService userLogService;

    @ApiOperation(value = "查询任务",httpMethod = "POST",protocols = "HTTP", produces ="application/json", notes = "查询任务")
    @PostMapping("/userlog")
    public Object selByJobId( Long job_id,Integer current, Integer size){
        return userLogService.selByJobId(job_id, current, size);
    }

    @ApiOperation(value = "根据日期查询分页",httpMethod = "POST",protocols = "HTTP", produces ="application/json", notes = "根据日期查询分页")
    @PostMapping("/selByJobIdAndDate")
    public Object selByJobIdAndDate(Long job_id,String date,Integer current, Integer size){
        return userLogService.selByJobIdAndDate(job_id,date, current, size);
    }
}
