package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysRealTimeMonitoring;
import com.cn.wavetop.dataone.entity.Userlog;
import com.cn.wavetop.dataone.entity.vo.SysMonitorRateVo;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface SysRealTimeMonitoringRepository extends JpaRepository<SysRealTimeMonitoring,Long>, JpaSpecificationExecutor<SysRealTimeMonitoring> {
    @Query("SELECT distinct jobId from SysRealTimeMonitoring")
    List<Long> selJobId();
    @Query("select e from SysRealTimeMonitoring e where e.jobId=:jobId and e.optTime>=:optTimeOld and e.optTime<:optTimeNew")
    List<SysRealTimeMonitoring> findByJobId(Long jobId, Date optTimeOld, Date optTimeNew);
     //取得速率中间数
    @Query(nativeQuery = true,value = "SELECT (count(*)+1) DIV 2 as b from  sys_real_time_monitoring s where s.job_id=:jobId and s.opt_time>=:optTimeOld and s.opt_time<:optTimeNew")
     Integer findByJobIdAndTime(Long jobId, Date optTimeOld, Date optTimeNew);

//    //取得读取速率的中间值
//     List<SysRealTimeMonitoring> findAll(Pageable pageable) ;
//
//    //取得写入速率的中间值
//    List<SysRealTimeMonitoring> findAll(Pageable pageable);
    @Modifying
    @Query("delete from SysRealTimeMonitoring s where s.jobId=:jobId and s.optTime>=:optTimeOld and s.optTime<:optTimeNew")
    int deleteByJobId(Long jobId,Date optTimeOld,Date optTimeNew);
}
