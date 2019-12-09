package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysMonitoring;
import com.cn.wavetop.dataone.entity.SysRela;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.Date;
import java.util.List;

@Repository
public interface SysMonitoringRepository extends JpaRepository<SysMonitoring,Long> {

    List<SysMonitoring> findByJobId(long job_id);
    List<SysMonitoring> findById(long id);
    List<SysMonitoring> findBySourceTableContainingAndJobId(String source_table,long job_id);
    List<SysMonitoring> findBySourceTableAndJobId(String source_table,long job_id);


    @Modifying
    @Query("delete from SysMonitoring where jobId = :job_id")
    int deleteByJobId(long job_id);


    @Modifying
    @Query("update SysMonitoring sm set sm.writeData = :writeData where sm.jobId = :id and sm.destTable = :table")
    void updateWriteMonitoring(long id, Long writeData,String table);
    @Modifying
    @Query("select sm from SysMonitoring sm where sm.jobId = :id and sm.destTable = :table")
    List<SysMonitoring> findByJobIdTable(long id,String table);
    @Modifying
    @Query("update SysMonitoring sm set sm.readData = :readData where sm.jobId = :id and sm.destTable = :table")
    void updateReadMonitoring(long id, Long readData,String table);

    @Query("SELECT distinct jobId from SysMonitoring")
    List<Long> selJobId();


    @Query(value="from SysMonitoring sd where sd.jobId=:job_id and sd.optTime >= :parse")
    List<SysMonitoring> findByIdAndDate(long job_id, Date parse);
}
