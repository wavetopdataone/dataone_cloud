package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysMonitoring;
import com.cn.wavetop.dataone.entity.SysTablerule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
public interface SysTableruleRepository extends JpaRepository<SysTablerule,Long> {

    List<SysTablerule> findBySourceTableAndJobId(String sourceTable,long jobId);
    List<SysTablerule> findByJobId(long job_id);
    List<SysTablerule> findById(long id);
    @Transactional
    @Modifying
    @Query("delete from SysTablerule where jobId = :job_id")
    int deleteByJobId(long job_id);


}
