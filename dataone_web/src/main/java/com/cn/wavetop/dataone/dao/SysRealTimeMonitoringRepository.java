package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysRealTimeMonitoring;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SysRealTimeMonitoringRepository extends JpaRepository<SysRealTimeMonitoring,Long> {

}
