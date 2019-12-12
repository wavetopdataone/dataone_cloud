package cn.com.wavetop.dataone_kafka.controller;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name="error")
public interface ErrorFeign {
    //为了导入Es数据 查询所有开启状态的库存集合
    @GetMapping("")
    public void insertErrorLog();
}
