package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.DataChangeSettings;
import com.cn.wavetop.dataone.entity.SysJobinfo;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SysJobinfoRespository  extends JpaRepository<SysJobinfo,Long> {
    List<SysJobinfo> findByJobId(long job_id);

    SysJobinfo findByJobId(Long job_id);

    boolean existsByJobId(Long jobId);

    int deleteByJobId(Long job_id);

}
