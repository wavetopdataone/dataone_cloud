package com.cn.wavetop.dataone.dao;

import com.cn.wavetop.dataone.entity.DataChangeSettings;
import com.cn.wavetop.dataone.entity.ErrorQueueSettings;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @Author yongz
 * @Date 2019/10/11、10:33
 */
@Repository
public interface ErrorQueueSettingsRespository  extends JpaRepository<ErrorQueueSettings,Long> {
    ErrorQueueSettings findByJobId(long job_id);

    List<ErrorQueueSettings> findAll();

    boolean existsByJobId(long jobId);

    int deleteByJobId(long job_id);
}
