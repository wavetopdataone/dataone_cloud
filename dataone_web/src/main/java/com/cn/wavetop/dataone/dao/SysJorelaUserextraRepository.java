package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.SysJorelaUserextra;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SysJorelaUserextraRepository extends JpaRepository<SysJorelaUserextra,Long> {
    List<SysJorelaUserextra> findByJobId(Long jobId);
}
