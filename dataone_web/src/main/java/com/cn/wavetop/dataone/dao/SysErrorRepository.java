package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysError;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SysErrorRepository extends JpaRepository<SysError,Long> {
}
