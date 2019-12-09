package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.config.SpringContextUtil;
import com.cn.wavetop.dataone.dao.*;
import com.cn.wavetop.dataone.entity.ErrorQueueSettings;
import com.cn.wavetop.dataone.entity.SysJobrela;
import com.cn.wavetop.dataone.entity.SysMonitoring;
import com.cn.wavetop.dataone.entity.SysUser;
import com.cn.wavetop.dataone.entity.vo.EmailJobrelaVo;
import com.cn.wavetop.dataone.entity.vo.EmailPropert;
import com.cn.wavetop.dataone.service.SysJobrelaService;
import com.cn.wavetop.dataone.util.EmailUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmailClient extends Thread {
    // 日志
    private static Logger log = LoggerFactory.getLogger(EmailClient.class); // 日志

//    private SysJobrelaService sysJobrelaService = (SysJobrelaService) SpringContextUtil.getBean("sysJobrelaServiceImpl");

    private SysJobrelaRespository repository = (SysJobrelaRespository) SpringContextUtil.getBean("sysJobrelaRespository");
    private ErrorQueueSettingsRespository errorQueueSettingsRespository = (ErrorQueueSettingsRespository) SpringContextUtil.getBean("errorQueueSettingsRespository");
    private SysMonitoringRepository sysMonitoringRepository = (SysMonitoringRepository) SpringContextUtil.getBean("sysMonitoringRepository");
    private SysUserJobrelaRepository sysUserJobrelaRepository = (SysUserJobrelaRepository) SpringContextUtil.getBean("sysUserJobrelaRepository");

    private SysUserRepository sysUserRepository = (SysUserRepository) SpringContextUtil.getBean("sysUserRepository");

    private boolean stopMe = true;

    @Override
    public void run() {
        List<EmailJobrelaVo> list = new ArrayList<>();
        ErrorQueueSettings errorQueueSettings = null;
        List<SysMonitoring> sysMonitoringList = new ArrayList<>();
        Optional<SysUser> sysUserOptional = null;
        List<SysUser> sysUserList = new ArrayList<>();
        EmailUtils emailUtils = new EmailUtils();
        EmailPropert emailPropert = null;
        Optional<SysJobrela> sysJobrela = null;
        while (stopMe) {
            double readData = 0;
            double errorData = 0;
            double result = 0;
            sysUserOptional = sysUserRepository.findById(Long.valueOf(1));
            list = repository.findEmailJobRelaUser();
            for (EmailJobrelaVo emailJobrelaVo : list) {
                sysUserList = sysUserJobrelaRepository.selUserNameByJobId(emailJobrelaVo.getJobId());
                emailJobrelaVo.setSysUserList(sysUserList);
                if (emailJobrelaVo.getErrorQueueAlert() == 1 || emailJobrelaVo.getErrorQueuePause() == 1) {
                    sysMonitoringList = sysMonitoringRepository.findByJobId(emailJobrelaVo.getJobId());
                    errorQueueSettings = errorQueueSettingsRespository.findByJobId(emailJobrelaVo.getJobId());
                    for (SysMonitoring sysMonitoring : sysMonitoringList) {
                        if (sysMonitoring.getReadData() != null && sysMonitoring.getErrorData() != null) {
                            if (sysMonitoring.getReadData() != 0 && sysMonitoring.getErrorData() != 0) {
                                readData = sysMonitoring.getReadData();
                                errorData = sysMonitoring.getErrorData();
                                result = errorData / readData;

                            } else {
                                result = 0;
                            }
                        } else {
                            result = 0;
                        }
                        System.out.println(errorQueueSettings.getWarnSetup() + "---" + result);
                        if (errorQueueSettings.getWarnSetup() < result) {

                            emailPropert = new EmailPropert();
                            emailPropert.setForm("上海浪擎科技有限公司");
                            emailPropert.setSubject("浪擎dataone错误预警通知：");
                            emailPropert.setMessageText("您参与的任务" + emailJobrelaVo.getJobrelaName() + "的" + sysMonitoring.getSourceTable() + "表的错误率为" + result * 100 + "%,特此通知");
                            emailPropert.setSag("您参与的任务" + emailJobrelaVo.getJobrelaName() + "的" + sysMonitoring.getSourceTable() + "表的错误率为" + result * 100 + "%,特此通知");
                            emailUtils.sendAuthCodeEmail(sysUserOptional.get(), emailPropert, emailJobrelaVo.getSysUserList());
                        }
                        if (errorQueueSettings.getPauseSetup() < result) {
                            sysJobrela = repository.findById(emailJobrelaVo.getJobId());
                            sysJobrela.get().setJobStatus("21");
                            repository.save(sysJobrela.get());
                            emailPropert = new EmailPropert();
                            emailPropert.setForm("上海浪擎科技有限公司");
                            emailPropert.setSubject("浪擎dataone错误暂停通知：");
                            emailPropert.setMessageText("您参与的任务" + emailJobrelaVo.getJobrelaName() + "的" + sysMonitoring.getSourceTable() + "表的错误率为" + result * 100 + "%,已经暂停此任务");
                            emailPropert.setSag("您参与的任务" + emailJobrelaVo.getJobrelaName() + "的" + sysMonitoring.getSourceTable() + "表的错误率为" + result * 100 + "%,已经暂停此任务");
                            emailUtils.sendAuthCodeEmail(sysUserOptional.get(), emailPropert, emailJobrelaVo.getSysUserList());
                        }
                    }
                }
            }
            try {
                Thread.sleep(5 * 60 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public void stopMe() {
        this.stopMe = false;
    }
}
