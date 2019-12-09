package com.cn.wavetop.dataone.service.impl;

import com.cn.wavetop.dataone.dao.UserLogRepository;
import com.cn.wavetop.dataone.entity.Userlog;
import com.cn.wavetop.dataone.entity.vo.ToData;
import com.cn.wavetop.dataone.service.UserLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserLogServiceImpl implements UserLogService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private UserLogRepository userLogRepository;

    @Override
    public Object selByJobId(long job_id) {
       List<Userlog> userlogList= userLogRepository.findByJobIdOrderByTimeDesc(job_id);
       return ToData.builder().status("1").data(userlogList).build();
    }


}
