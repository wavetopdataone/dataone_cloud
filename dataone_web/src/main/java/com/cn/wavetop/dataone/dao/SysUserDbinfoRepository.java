package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysUserDbinfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SysUserDbinfoRepository extends JpaRepository<SysUserDbinfo,Long> {
    int deleteByDbinfoId(Long dbinfoId);
}
