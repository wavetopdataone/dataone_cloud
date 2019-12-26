package com.cn.wavetop.dataone.controller;

import com.alibaba.fastjson.JSON;
import com.cn.wavetop.dataone.service.UserLogService;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.rmi.NotBoundException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/userlog")
public class UserLogController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

    @ApiOperation(value = "查询系统错误日志", httpMethod = "POST", protocols = "HTTP", produces = "application/json", notes = "查询系统错误日志")
    @PostMapping("/errorShow")
    public void showError(HttpServletRequest request, HttpServletResponse response, String date) {
        response.setContentType("text/plain");
        FileInputStream in = null;
        DateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
        File file = new File("dataoneerror-" + date + ".0.log");
        OutputStream out = null;
        try {
            out = response.getOutputStream();
            if (!file.exists()) {
                response.setContentType("application/json; charset=utf-8");
                Map<Object,Object> map=new HashMap<>();
                map.put("status", "404");
                map.put("message", "系统找不到指定文件！");
                String a= String.valueOf(JSON.toJSON(map));
                byte[]b=a.getBytes();
                for(int i=0;i<b.length;i++){
                    out.write(b[i]);
                }
            }else {
                in = new FileInputStream(file);
                byte[] b = new byte[512];
                while (true) {
                    if (!((in.read(b)) != -1)) break;
                    out.write(b);
                }
            }
            out.flush();
        } catch (IOException e) {
            logger.error("*IO错误",e);
            e.printStackTrace();
        }finally {
            if(in!=null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @ApiOperation(value = "系统错误日志下载", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "系统错误日志下载")
    @GetMapping("/OutputError")
    public void showError2(HttpServletRequest request, HttpServletResponse response, String date) {
        FileInputStream in = null;
        DateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
        String fileName = "dataoneerror-" + date + ".0.log";
//        String fileName2 = "dataoneerror-" + date + ".1.log";
        String FileNewName = "dataoneError-" + date + ".txt";
        OutputStream out = null;
        response.setContentType("text/plain");

        File file = new File(fileName);

        try {
            out = response.getOutputStream();
            if (!file.exists()) {
                response.setContentType("application/json; charset=utf-8");
                Map<Object,Object> map=new HashMap<>();
                map.put("status", "404");
                map.put("message", "系统找不到指定文件！");
                String a= String.valueOf(JSON.toJSON(map));
                byte[]b=a.getBytes();
                for(int i=0;i<b.length;i++){
                    out.write(b[i]);
                }
            }else {
                response.setHeader("Content-Disposition",
                        "attachment;fileName=" + FileNewName);
                in = new FileInputStream(file);
                byte[] b = new byte[512];
                while (true) {
                    if (!((in.read(b)) != -1)) break;
                    out.write(b);
                }

            }
            out.flush();
        } catch (IOException e) {
            logger.error("*IO错误",e);
            e.printStackTrace();
        }finally {
            if(in!=null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}
