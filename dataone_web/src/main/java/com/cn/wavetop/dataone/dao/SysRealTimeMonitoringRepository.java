package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysRealTimeMonitoring;
import com.cn.wavetop.dataone.entity.vo.SysMonitorRateVo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface SysRealTimeMonitoringRepository extends JpaRepository<SysRealTimeMonitoring,Long> {
    @Query("SELECT distinct jobId from SysRealTimeMonitoring")
    List<Long> selJobId();
    @Query("select e from SysRealTimeMonitoring e where e.jobId=:jobId and e.optTime>=:optTimeOld and e.optTime<:optTimeNew")
    List<SysRealTimeMonitoring> findByJobId(Long jobId, Date optTimeOld, Date optTimeNew);
     //取得速率的峰值
    @Query("select new com.cn.wavetop.dataone.entity.vo.SysMonitorRateVo(max(s.readRate) ,max(s.writeRate))  from SysRealTimeMonitoring s where s.jobId=:jobId and s.optTime>=:optTimeOld and s.optTime<:optTimeNew")
     List<SysMonitorRateVo> findByJobIdAndTime(Long jobId, Date optTimeOld, Date optTimeNew);
    @Modifying
    @Query("delete from SysRealTimeMonitoring s where s.jobId=:jobId and s.optTime>=:optTimeOld and s.optTime<:optTimeNew")
    int deleteByJobId(Long jobId,Date optTimeOld,Date optTimeNew);
}
