package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.service.UserLogService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/userlog")
public class UserLogController {

    @Autowired
    private UserLogService userLogService;

    @ApiOperation(value = "查询任务", httpMethod = "POST", protocols = "HTTP", produces = "application/json", notes = "查询任务")
    @PostMapping("/userlog")
    public Object selByJobId(Long job_id, Integer current, Integer size) {
        return userLogService.selByJobId(job_id, current, size);
    }

    @ApiOperation(value = "根据日期查询分页", httpMethod = "POST", protocols = "HTTP", produces = "application/json", notes = "根据日期查询分页")
    @PostMapping("/selByJobIdAndDate")
    public Object selByJobIdAndDate(Long job_id, String date, Integer current, Integer size) {
        return userLogService.selByJobIdAndDate(job_id, date, current, size);
    }

    @ApiOperation(value = "技术支持发送邮件", httpMethod = "POST", protocols = "HTTP", produces = "application/json", notes = "技术详情发送邮件")
    @PostMapping("/emailSupport")
    public Object supportEmail(Long userlogId) {
        return userLogService.supportEmail(userlogId);
    }

    @ApiOperation(value = "系统错误日志", httpMethod = "POST", protocols = "HTTP", produces = "application/json", notes = "系统错误日志")
    @PostMapping("/errorShow")
    public void showError(HttpServletRequest request, HttpServletResponse response) {
        response.setContentType("application/txt");
        FileInputStream in = null;
        Date date = new Date();
        DateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
        try {
            in = new FileInputStream(new File("dataoneerror.log." + ft.format(date) + ".log"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        OutputStream out = null;
        try {
            out = response.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] b = new byte[512];
        while (true) {
            try {
                if (!((in.read(b)) != -1)) break;
                out.write(b);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        try {
            out.flush();
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
