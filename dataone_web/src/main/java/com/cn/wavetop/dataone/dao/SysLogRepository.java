package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysLog;
import com.cn.wavetop.dataone.entity.SysUserlog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SysLogRepository extends JpaRepository<SysLog,Long>,JpaSpecificationExecutor<SysLog> {

    Page<SysLog> findAll(Pageable pageable);
    @Query("select l from SysLog l order by l.createDate desc")
    List<SysLog> findAll();

    List<SysLog> findByJobIdOrderByCreateDateDesc(Long jobId);

    List<SysLog> findByDeptName(String deptName, Pageable pageable);
    List<SysLog> findByDeptNameOrderByCreateDateDesc(String deptName);

    Integer countByDeptName(String deptName);
}
