package com.cn.wavetop.dataone.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity // 标识实体
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SysFilterTable {
    @Id // 标识主键
    @GeneratedValue(strategy = GenerationType.IDENTITY) // 自定义生成
    private Long id;
    private Long jobId;
    private String filterTable;
    private String filterField;

}
