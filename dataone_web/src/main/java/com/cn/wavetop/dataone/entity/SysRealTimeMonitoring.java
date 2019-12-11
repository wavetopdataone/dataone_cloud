package com.cn.wavetop.dataone.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.Date;

@Entity // 标识实体
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SysRealTimeMonitoring {
    @Id // 标识主键
    @GeneratedValue(strategy = GenerationType.IDENTITY) // 自定义生成
    private Long id;
    private Double readRate;//读取速率
    private Double writeRate;//写入速率
    private Long readAmount;//读取量
    private Long writeAmount;//写入量
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date optTime;
    private Long jobId;
    private String destTable;

}
