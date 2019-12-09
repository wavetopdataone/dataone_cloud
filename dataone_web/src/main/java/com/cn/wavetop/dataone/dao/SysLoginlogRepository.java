package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysDept;
import com.cn.wavetop.dataone.entity.SysLog;
import com.cn.wavetop.dataone.entity.SysLoginlog;
import com.cn.wavetop.dataone.entity.SysUserlog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;


import java.util.List;
import java.util.Optional;
@Repository
public interface SysLoginlogRepository extends JpaRepository<SysLoginlog,Long>, JpaSpecificationExecutor<SysLoginlog> {
    Page<SysLoginlog> findAll(Pageable pageable);


    List<SysLoginlog> findByDeptName(String deptName, Pageable pageable);
    Integer countByDeptName(String deptName);


    List<SysLoginlog> findAll();

    List<SysLoginlog> findByDeptNameOrderByCreateDateDesc(String deptName);


   // List<SysLoginlog> findAll(Specification<SysLoginlog> querySpecifi);


}
