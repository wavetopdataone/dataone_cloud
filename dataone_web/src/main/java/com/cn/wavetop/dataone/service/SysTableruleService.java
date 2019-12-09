package com.cn.wavetop.dataone.service;

import com.cn.wavetop.dataone.entity.SysDbinfo;
import com.cn.wavetop.dataone.entity.SysTablerule;

import java.sql.SQLException;

public interface SysTableruleService {
    Object tableruleAll();
    Object checkTablerule(long job_id);
    Object addTablerule(SysTablerule sysTablerule);
    Object editTablerule(SysTablerule sysTablerule);
    Object deleteTablerule(long job_id);
    Object linkDataTable(SysDbinfo sysDbinfo);
}
