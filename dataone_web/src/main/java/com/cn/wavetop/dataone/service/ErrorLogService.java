package com.cn.wavetop.dataone.service;

import com.cn.wavetop.dataone.entity.ErrorLog;

/**
 * @Author yongz
 * @Date 2019/10/11、11:17
 */
public interface ErrorLogService {
    Object getErrorlogAll();

    Object getCheckError(Long jobId,String tableName,String type,String startTime,String endTime,String context);

    Object addErrorlog(ErrorLog errorLog);

    Object editErrorlog(ErrorLog errorLog);

    Object deleteErrorlog(String ids);

    Object queryErrorlog(Long jobId);
    Object selErrorlogById(Long id);
}
