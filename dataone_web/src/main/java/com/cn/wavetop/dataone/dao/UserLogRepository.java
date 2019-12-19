package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.Userlog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserLogRepository extends JpaRepository<Userlog,Integer> {

    List<Userlog> findByJobIdOrderByTimeDesc(long job_id);


}
