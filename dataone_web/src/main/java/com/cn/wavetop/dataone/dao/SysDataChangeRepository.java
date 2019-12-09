package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysDataChange;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface SysDataChangeRepository extends JpaRepository<SysDataChange,Long> {
    SysDataChange findByJobIdAndCreateTime(Long jobId, Date createTime);
    List<SysDataChange> findByJobId(Long jobId);
    @Query("select s from SysDataChange s where s.jobId=:jobId and s.createTime>:createTime ")
    List<SysDataChange> findByJobIdAndTime(Long jobId, Date createTime);
    @Query(value="from SysDataChange sd where sd.jobId=:job_id and sd.createTime = :parse")
    SysDataChange findByJobIdAndDate(long job_id,Date parse);
    int deleteByJobId(Long jobId);
}
