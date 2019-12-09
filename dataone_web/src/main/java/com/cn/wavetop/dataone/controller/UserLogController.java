package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.service.UserLogService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/userlog")
public class UserLogController {

    @Autowired
    private UserLogService userLogService;

    @ApiOperation(value = "查询任务",protocols = "HTTP", produces ="application/json", notes = "查询任务")
    @ApiImplicitParam(name = "job_id", value = "job_id", dataType = "long")
    @RequestMapping("/userlog")
    public Object selByJobId( long job_id){
        System.out.println(job_id);
        return userLogService.selByJobId(job_id);
    }
}
