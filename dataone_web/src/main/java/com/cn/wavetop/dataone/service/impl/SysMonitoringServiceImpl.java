package com.cn.wavetop.dataone.service.impl;

import com.cn.wavetop.dataone.config.shiro.MyShiroRelam;
import com.cn.wavetop.dataone.dao.*;
import com.cn.wavetop.dataone.entity.*;
import com.cn.wavetop.dataone.entity.vo.CountAndTime;
import com.cn.wavetop.dataone.entity.vo.ToData;
import com.cn.wavetop.dataone.entity.vo.ToDataMessage;
import com.cn.wavetop.dataone.service.SysMonitoringService;
import com.cn.wavetop.dataone.util.DateUtil;
import org.hibernate.dialect.Sybase11Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.transaction.Transactional;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class SysMonitoringServiceImpl implements SysMonitoringService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SysMonitoringRepository sysMonitoringRepository;
    @Autowired
    private SysTableruleRepository sysTableruleRepository;
    @Autowired
    private SysDataChangeRepository sysDataChangeRepository;
    @Autowired
    private SysJobrelaRespository sysJobrelaRespository;
    @Override
    public Object findAll() {
        List<SysMonitoring> sysUserList = sysMonitoringRepository.findAll();
        return ToData.builder().status("1").data(sysUserList).build();
    }

    @Override
    public Object findByJobId(long job_id) {
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(job_id);
        System.out.println(sysMonitoringList);
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            return ToData.builder().status("1").data(sysMonitoringList).build();
        } else {
            return ToDataMessage.builder().status("0").message("没有找到").build();
        }
    }

    @Transactional
    @Override
    public Object update(SysMonitoring sysMonitoring) {
        try {
            List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(sysMonitoring.getJobId());
            System.out.println(sysMonitoringList);
            List<SysMonitoring> userList = new ArrayList<SysMonitoring>();
            if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
                //sysMonitoringList.get(0).setId(sysMonitoring.getId());
                sysMonitoringList.get(0).setJobId(sysMonitoring.getJobId());
                sysMonitoringList.get(0).setJobName(sysMonitoring.getJobName());
                sysMonitoringList.get(0).setSyncRange(sysMonitoring.getSyncRange());
                sysMonitoringList.get(0).setSourceTable(sysMonitoring.getSourceTable());
                sysMonitoringList.get(0).setDestTable(sysMonitoring.getDestTable());
                sysMonitoringList.get(0).setSqlCount(sysMonitoring.getSqlCount());
                sysMonitoringList.get(0).setOptTime(sysMonitoring.getOptTime());
                sysMonitoringList.get(0).setNeedTime(sysMonitoring.getNeedTime());
                sysMonitoringList.get(0).setFulldataRate(sysMonitoring.getFulldataRate());
                sysMonitoringList.get(0).setIncredataRate(sysMonitoring.getIncredataRate());
                sysMonitoringList.get(0).setStocksdataRate(sysMonitoring.getStocksdataRate());
                sysMonitoringList.get(0).setTableRate(sysMonitoring.getTableRate());
                sysMonitoringList.get(0).setReadRate(sysMonitoring.getReadRate());
                sysMonitoringList.get(0).setDisposeRate(sysMonitoring.getDisposeRate());
                sysMonitoringList.get(0).setJobStatus(sysMonitoring.getJobStatus());
                sysMonitoringList.get(0).setReadData(sysMonitoring.getReadData());
                sysMonitoringList.get(0).setWriteData(sysMonitoring.getWriteData());
                sysMonitoringList.get(0).setErrorData(sysMonitoring.getErrorData());

                SysMonitoring user = sysMonitoringRepository.save(sysMonitoringList.get(0));
                System.out.println(user);
                userList = sysMonitoringRepository.findById(user.getId());
                if (user != null && !"".equals(user)) {
                    return ToData.builder().status("1").data(userList).message("修改成功").build();
                } else {
                    return ToDataMessage.builder().status("0").message("修改失败").build();
                }

            } else {
                return ToDataMessage.builder().status("0").message("修改失败").build();

            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }

    }

    @Transactional
    @Override
    public Object addSysMonitoring(SysMonitoring sysMonitoring) {
        try {
            System.out.println(sysMonitoring + "-----------" + sysMonitoring.getJobId());
            if (sysMonitoringRepository.findByJobId(sysMonitoring.getJobId()) != null && sysMonitoringRepository.findByJobId(sysMonitoring.getJobId()).size() > 0) {

                return ToDataMessage.builder().status("0").message("已存在").build();
            } else {
                SysMonitoring user = sysMonitoringRepository.save(sysMonitoring);
                List<SysMonitoring> userList = new ArrayList<SysMonitoring>();
                userList.add(user);
                return ToData.builder().status("1").data(userList).message("添加成功").build();
            }

        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }

    }

    @Transactional
    @Override
    public Object delete(long job_id) {
        try {
            List<SysMonitoring> sysUserList = sysMonitoringRepository.findByJobId(job_id);
            if (sysUserList != null && sysUserList.size() > 0) {
                int result = sysMonitoringRepository.deleteByJobId(job_id);
                if (result > 0) {
                    return ToDataMessage.builder().status("1").message("删除成功").build();
                } else {
                    return ToDataMessage.builder().status("0").message("删除失败").build();
                }
            } else {
                return ToDataMessage.builder().status("0").message("任务不存在").build();
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }


    }

    @Override
    public Object findLike(String source_table, long job_id) {
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findBySourceTableContainingAndJobId(source_table, job_id);
        return ToData.builder().status("1").data(sysMonitoringList).build();
    }

    @Override
    public Object dataRate(long job_id) {
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(job_id);
        List<Object> stringList = new ArrayList<Object>();
        CountAndTime countAndTime = new CountAndTime();
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            for (SysMonitoring s : sysMonitoringList) {
                countAndTime.setSqlCount(s.getSqlCount());
                countAndTime.setOpTime(s.getOptTime());
                stringList.add(countAndTime);
//                stringList.add(s.getSqlCount());
//                stringList.add(s.getOptTime());
                System.out.println(s.getOptTime());
            }
            return ToData.builder().status("1").data(stringList).build();
        } else {
            return ToDataMessage.builder().status("0").message("没有找到").build();
        }
    }

    @Override
    public Object showMonitoring(long job_id) {
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(job_id);
        //StringBuffer sum=new StringBuffer();
        double sum = 0;
        double errorDatas = 0;
        double readData = 0;
        double writeData = 0;
        double readRate = 0;
        double disposeRate = 0;
        double synchronous = 0;
        long index = 0;
        long index1 = 0;
        HashMap<Object, Object> map = new HashMap();
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            for (SysMonitoring sysMonitoring : sysMonitoringList) {
                if (sysMonitoring.getSqlCount() == null) {
                    sysMonitoring.setSqlCount((long) 0);
                }
                if (sysMonitoring.getReadData() == null) {
                    sysMonitoring.setReadData((long) 0);
                }
                if (sysMonitoring.getWriteData() == null) {
                    sysMonitoring.setWriteData((long) 0);
                }
                if (sysMonitoring.getErrorData() == null) {
                    sysMonitoring.setErrorData((long) 0);
                }
                readData += sysMonitoring.getReadData();
                writeData += sysMonitoring.getWriteData();
                errorDatas += sysMonitoring.getErrorData();
                if (sysMonitoring.getReadRate() == null || sysMonitoring.getReadRate() == 0) {
                    sysMonitoring.setReadRate((long) 0);
                    index++;
                }
                if (sysMonitoring.getDisposeRate() == null || sysMonitoring.getDisposeRate() == 0) {
                    sysMonitoring.setDisposeRate((long) 0);
                    index1++;
                }
                readRate += sysMonitoring.getReadRate();
                disposeRate += sysMonitoring.getDisposeRate();

            }
            if ((sysMonitoringList.size() - index) != 0) {
                readRate = readRate / (sysMonitoringList.size() - index);
            }
            if ((sysMonitoringList.size() - index1) != 0) {
                disposeRate = disposeRate / (sysMonitoringList.size() - index1);
            }

            if (readData != 0) {
                synchronous = writeData / readData;
            }
            System.out.println(disposeRate + "-----" + readRate);
            map.put("read_datas", readData);
            map.put("write_datas", writeData);
            map.put("error_datas", errorDatas);
            map.put("read_rate", readRate);
            map.put("dispose_rate", disposeRate);
            map.put("synchronous", synchronous);
            map.put("status", "1");

        } else {
            map.put("read_datas", "0");
            map.put("write_datas", "0");
            map.put("error_datas", "0");
            map.put("read_rate", "0");
            map.put("dispose_rate", "0");
            map.put("synchronous", "0");
            map.put("status", "1");

        }
        //把同步速率更新到任务表用于首页的显示
       SysJobrela sysJobrela= sysJobrelaRespository.findById(job_id);
        sysJobrela.setJobRate(synchronous);
        sysJobrelaRespository.save(sysJobrela);
        return map;
    }

    @Transactional
    @Override
    public Object tableMonitoring(long job_id) {
        try {
            List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(job_id);
            List<SysTablerule> sysTablerules = new ArrayList<SysTablerule>();
            List<SysMonitoring> sysMonitoringList1 = new ArrayList<SysMonitoring>();

            if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
                for (SysMonitoring sysMonitoring : sysMonitoringList) {
                    sysTablerules = sysTableruleRepository.findBySourceTableAndJobId(sysMonitoring.getSourceTable(), sysMonitoring.getJobId());
                    sysMonitoringList1 = sysMonitoringRepository.findBySourceTableAndJobId(sysMonitoring.getSourceTable(), sysMonitoring.getJobId());
                    System.out.println(sysTablerules);
                    //如果目的表没有，去tablerule中找（找到的一定是修改过的）目标表，插到监控表里
                    // 如果tablerule中没有则代表源表和目的表是一致的;
                    if (sysMonitoringList1 != null && sysMonitoringList1.size() > 0) {
                        if (sysTablerules != null && sysTablerules.size() > 0) {
                            sysMonitoringList1.get(0).setDestTable(sysTablerules.get(0).getSourceTable());

                        } else {
                            sysMonitoringList1.get(0).setDestTable(sysMonitoringList1.get(0).getSourceTable());
                        }
                        sysMonitoringRepository.save(sysMonitoringList1.get(0));
                    }
////                    for (SysTablerule sysTablerule:sysTablerules){
////                        System.out.println(sysMonitoringList1);
////                        sysMonitoringList1.get(0).setDestTable(sysTablerule.getDestTable());
////                        SysMonitoring S= sysMonitoringRepository.save(sysMonitoringList1.get(0));
////                        System.out.println(S);
////                    }
                }
                sysMonitoringList = sysMonitoringRepository.findByJobId(job_id);
                return ToData.builder().status("1").data(sysMonitoringList).build();
            } else {
                return ToDataMessage.builder().status("0").message("没有查到数据").build();
            }

        } catch (Exception e) {
            return ToDataMessage.builder().status("0").message("发生错误").build();

        }

    }

    @Override
    public Object SyncMonitoring(Long jobId, String num) {
        Date date = new Date();
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式

//        SimpleDateFormat df = new SimpleDateFormat("MM-dd");// 设置日期格式

        Map<String, List<String>> map = new HashMap<>();
        Map<Object, Double> map2 = new HashMap<>();
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        List<String> list3 = new ArrayList<>();
        List<String> list4 = new ArrayList<>();
        List<String> list5 = new ArrayList<>();
        String ab = dfs.format(date);
        String cd = dfs.format(date);
        String sum = "-" + num;
        Integer r = Integer.parseInt(sum) + 1;
        ab = DateUtil.dateAdd(ab, r);
        for (int i = r; i < 0; i++) {
            String ef = DateUtil.dateAdd(cd, i);
            list1.add(ef);
            String str = ef.substring(5, 7) + "." + ef.substring(8, 10);
            list2.add(str);//09.25
        }
        System.out.println(DateUtil.StringToDate(ab));
        List<SysDataChange> sysDataChanges = sysDataChangeRepository.findByJobIdAndTime(jobId, DateUtil.StringToDate(ab));
        if (sysDataChanges != null && sysDataChanges.size() > 0) {
            for (SysDataChange sysDataChange : sysDataChanges) {
                for (int i = 0; i < list1.size(); i++) {
                    String time = String.valueOf(sysDataChange.getCreateTime());
                    if (time.substring(0, 10).equals(list1.get(i))) {
//                      list2.add(String.valueOf(sysDataChange.getCreateTime()));
                        list3.add(String.valueOf(sysDataChange.getReadData()));
                        list4.add(String.valueOf(sysDataChange.getWriteData()));
                        list5.add(String.valueOf(sysDataChange.getErrorData()));
                    } else {
                        list3.add("0");
                        list4.add("0");
                        list5.add("0");
                    }
                }
            }
            map2 = (HashMap<Object, Double>) showMonitoring(jobId);
            System.out.println(map2);
            String nowdate = dfs.format(new Date());
            String strss = nowdate.substring(5, 7) + "." + nowdate.substring(8, 10);
            list2.add(strss);
            list3.add(String.valueOf(map2.get("read_datas")));
            list4.add(String.valueOf(map2.get("write_datas")));
            list5.add(String.valueOf(map2.get("error_datas")));

            map.put("data1", list1);
            map.put("data2", list2);
            map.put("data3", list3);
            map.put("data4", list4);
            map.put("data5", list5);
            System.out.println(map);
        }
        return map;
    }

    /**
     * 更新读监听数据
     *
     * @param readData
     */
    @Transactional
    @Override
    public void updateReadMonitoring(long id, Long readData, String table) {
        //List<SysMonitoring> byId = sysMonitoringRepository.findById(id);
//        String table = "TEST";
        sysMonitoringRepository.updateReadMonitoring(id, readData, table);
    }

    /**
     * 更新写监听数据
     */
    @Transactional
    @Override
    public void  updateWriteMonitoring(long id, Long writeData, String table) {
//        String table = "TEST";

        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobIdTable(id, table);
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            for (SysMonitoring sysMonitoring : sysMonitoringList) {
                try {
                    Long readData = sysMonitoring.getReadData();
                    System.out.println("readData = " + readData);
                    System.out.println("writeData = " + writeData);
                    /*Long errorData = readData - writeData;
                    System.out.println("errorData = " + errorData);*/
                    sysMonitoringRepository.updateWriteMonitoring(id, writeData, table);
                } catch (Exception e) {

                }
            }
        }

    }

    /**
     * 折线图数据
     *
     * @param job_id
     * @return
     */
    @Transactional
    @Override
    public Object dataChangeView(long job_id, Integer date) {
        HashMap<String, List> map1 = new HashMap<>();
        HashMap<String, List> map2 = new HashMap<>();
        HashMap<String, List> map3 = new HashMap<>();
        HashMap<String, List> map4 = new HashMap<>();
        List<Map> list = new ArrayList<>();
        //定义一个list集合，存放过去date天
        List<String> days = new ArrayList<>();
        //定义一个list集合，存放过去date天每天的数据量
        List<Long> writes = new ArrayList<>();
        List<Long> reads = new ArrayList<>();
        List<Long> errors = new ArrayList<>();
        //获取日历对象
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -6);
       /* //获取每天的时间
        Date time = calendar.getTime();
        //获取每天
        String dayd= new SimpleDateFormat("yyyy-MM-dd").format(time);
        SysDataChange sysDataChange = sysDataChangeRepository.findByJobIdAndDate(job_id,dayd);*/

        for (int i = 0; i < 7; i++) {
            Long x = 0L;
            //获取每天的时间
            Date time = calendar.getTime();
            //获取每天
            String dayd = new SimpleDateFormat("yyyy-MM-dd").format(time);
            String daya = new SimpleDateFormat("MM-dd").format(time);
            Date parse = null;
            try {
                parse = new SimpleDateFormat("yyyy-MM-dd").parse(dayd);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if (i < 6) {
                SysDataChange sysDataChange = sysDataChangeRepository.findByJobIdAndDate(job_id, parse);
                System.out.println("sysDataChange = " + sysDataChange);
                if (null != sysDataChange) {
                    writes.add(sysDataChange.getWriteData());
                    reads.add(sysDataChange.getReadData());
                    errors.add(sysDataChange.getErrorData());
                } else {
                    writes.add(x);
                    reads.add(x);
                    errors.add(x);
                }
            } else {
                List<SysMonitoring> sysMonitorings = sysMonitoringRepository.findByIdAndDate(job_id, parse);

                if (sysMonitorings != null && sysMonitorings.size() > 0) {
                    Long writeData = 0L;
                    Long readData = 0L;
                    Long errorData = 0L;
                    for (SysMonitoring sysMonitoring : sysMonitorings) {

                        if (null != sysMonitoring) {
                            if (sysMonitoring.getWriteData() != null) {

                                writeData += sysMonitoring.getWriteData();

                            }
                            if (sysMonitoring.getReadData() != null) {
                                readData += sysMonitoring.getReadData();

                            }
                            if (sysMonitoring.getErrorData() != null) {
                                errorData += sysMonitoring.getErrorData();

                            }
                        }/*else {
                            writes.add(x);
                            reads.add(x);
                            errors.add(x);
                        }*/
                    }
                    writes.add(/*sysMonitoring.getWriteData()*/writeData);
                    reads.add(/*sysMonitoring.getReadData()*/readData);
                    errors.add(/*sysMonitoring.getErrorData()*/errorData);
                } else {
                    writes.add(x);
                    reads.add(x);
                    errors.add(x);
                }
            }
            //添加每一天
            days.add(daya);

            //每次循环都在日历的天数+1
            calendar.add(Calendar.DATE, +1);

        }
        map1.put("data", days);
        map2.put("data", writes);
        map3.put("data", reads);
        map4.put("data", errors);
        list.add(map1);
        list.add(map2);
        list.add(map3);
        list.add(map4);
        return list;
    }
}
