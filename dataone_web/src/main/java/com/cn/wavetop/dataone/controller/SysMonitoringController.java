package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.dao.SysDataChangeRepository;
import com.cn.wavetop.dataone.dao.SysMonitoringRepository;
import com.cn.wavetop.dataone.entity.SysDataChange;
import com.cn.wavetop.dataone.entity.SysMonitoring;
import com.cn.wavetop.dataone.entity.SysRela;
import com.cn.wavetop.dataone.service.SysMonitoringService;
import com.cn.wavetop.dataone.service.SysRelaService;
import com.cn.wavetop.dataone.util.DateUtil;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/sys_monitoring")
public class SysMonitoringController {
    @Autowired
    private SysMonitoringService sysMonitoringService;
    @Autowired
    private SysMonitoringRepository sysMonitoringRepository;
    @Autowired
    private SysDataChangeRepository sysDataChangeRepository;


    @ApiOperation(value = "查看全部", protocols = "HTTP", produces = "application/json", notes = "查看全部")
    @RequestMapping("/monitoring_all")
    public Object userAll(){
        return sysMonitoringService.findAll();
    }
    @ApiOperation(value = "单一查询", protocols = "HTTP", produces = "application/json", notes = "单一查询")
    @RequestMapping("/check_monitoring")
    public Object checkUser(long job_id){
        return sysMonitoringService.findByJobId(job_id);
    }
    @ApiOperation(value = "添加", protocols = "HTTP", produces = "application/json", notes = "添加")
    @PostMapping("/add_monitoring")
    public Object addUser(@RequestBody SysMonitoring sysMonitoring){
        return sysMonitoringService.addSysMonitoring(sysMonitoring);
    }
    @ApiOperation(value = "修改", protocols = "HTTP", produces = "application/json", notes = "修改")
    @PostMapping("/edit_monitoring")
    public Object editUser(@RequestBody SysMonitoring sysMonitoring){
        return sysMonitoringService.update(sysMonitoring);
    }
    @ApiOperation(value = "删除", protocols = "HTTP", produces = "application/json", notes = "删除")
    @RequestMapping("/delete_monitoring")
    public Object deleteUser(long job_id){
        return sysMonitoringService.delete(job_id);
    }

    @ApiOperation(value = "模糊查询", protocols = "HTTP", produces = "application/json", notes = "模糊查询")
    @RequestMapping("/query_monitoring")
    public Object queryMonitoring(String source_table,long job_id){
        return sysMonitoringService.findLike(source_table,job_id);
    }
    @ApiOperation(value = "根据jobid查询条数和时间", protocols = "HTTP", produces = "application/json", notes = "根据jobid查询条数和时间")
    @RequestMapping("/data_rate")
    public Object dataRate(long job_id){
        return sysMonitoringService.dataRate(job_id);
    }
    @ApiOperation(value = "根据jobId查询读取写入错误行", protocols = "HTTP", produces = "application/json", notes = "根据jobId查询读取写入错误行")
    @RequestMapping("/show_monitoring")
    public Object showMonitoring(long job_id){
        return sysMonitoringService.showMonitoring(job_id);
    }
    @ApiOperation(value = "插入目标表名,显示该记录", protocols = "HTTP", produces = "application/json", notes = "插入目标表名,显示该记录")
    @RequestMapping("/table_monitoring")
    public Object tableMonitoring(long job_id){
        return sysMonitoringService.tableMonitoring(job_id);
    }
    @PostMapping("/syncMonitoring")
    public  Object SyncMonitoring(Long jobId,String num){

        return sysMonitoringService.SyncMonitoring(jobId,num);
    }
    /**
     * 折线图数据
     * @param job_id
     * @return
     */
    @ApiOperation(value = "折线图数据", protocols = "POST", produces = "application/json", notes = "折线图数据")
    @PostMapping("/dataChangeView")
    public Object dataChangeView(@RequestParam long job_id,@RequestParam Integer date){
        return sysMonitoringService.dataChangeView(job_id,date);
    }


    @Scheduled(cron = "0 58 23 * * ?")
    public void saveDataChange() {
        SysDataChange dataChange = null;
        HashMap<Object, Double> map = new HashMap<>();
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");// 设置日期格式
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
        long errorData = 0;//错误量
        long readData = 0;//读取量
        long writeData = 0;//写入量
        long readRate = 0;//读取速率
        long disposeRate = 0;//处理速率
        long lastReadData = 0;//前面天数的读取总量
        long lastWriteData = 0;//前面天数的写入总量
        long lastErrorData = 0;//前面天数的错误总量
//        String nowTime = df.format(new Date());//当前时间
        String nowDate = dfs.format(new Date());//几号
//        String hour = nowDate.substring(0, 2);
//        String minue = nowDate.substring(3, 5);
        String yesterDay = DateUtil.dateAdd(nowDate, -1);//昨天
        String weekDay = DateUtil.todate(nowDate);//星期几
//        String now = "";
//        String last = "";
////            if ("23".equals(hour)&&"59".equals(minue)) {
////                System.out.println(new Date() + "起始时间：");
//        last = df.format(new Date());
        List<Long> jobIdList = sysMonitoringRepository.selJobId();//查询所有jobid
        if (jobIdList != null && jobIdList.size() > 0) {
            for (Long jobId : jobIdList) {
                //根据jobid查询读写错误，处理写入值
                map = (HashMap<Object, Double>) sysMonitoringService.showMonitoring(jobId);
                dataChange = new SysDataChange();
                dataChange = sysDataChangeRepository.findByJobIdAndCreateTime(jobId, DateUtil.StringToDate(yesterDay));
                //如果前一天有值
                if (dataChange != null) {
                    lastReadData = map.get("read_datas").longValue();//前面天数的读取总量
                    lastWriteData = map.get("write_datas").longValue();//前面天数的写入总量
                    lastErrorData = map.get("error_datas").longValue();//前面天数的错误总量
                    readData = map.get("read_datas").longValue() - dataChange.getLastReadData();
                    writeData = map.get("write_datas").longValue() - dataChange.getLastWriteData();
                    errorData = map.get("error_datas").longValue() - dataChange.getLastErrorData();
                } else {
                    readData = map.get("read_datas").longValue();
                    writeData = map.get("write_datas").longValue();
                    errorData = map.get("error_datas").longValue();
                    lastReadData = map.get("read_datas").longValue();//前面天数的读取总量
                    lastWriteData = map.get("write_datas").longValue();//前面天数的写入总量
                    lastErrorData = map.get("error_datas").longValue();//前面天数的错误总量
                }
                readRate = map.get("read_rate").longValue();
                disposeRate = map.get("dispose_rate").longValue();
                SysDataChange dataChange2 = new SysDataChange();
                dataChange2.setCreateTime(DateUtil.StringToDate(nowDate));
                dataChange2.setDisposeRate(disposeRate);
                dataChange2.setJobId(jobId);
                dataChange2.setWeekDay(weekDay);
                dataChange2.setErrorData(errorData);
                dataChange2.setReadData(readData);
                dataChange2.setWriteData(writeData);
                dataChange2.setReadRate(readRate);
                dataChange2.setLastReadData(lastReadData);
                dataChange2.setLastWriteData(lastWriteData);
                dataChange2.setLastErrorData(lastErrorData);
                sysDataChangeRepository.save(dataChange2);
            }
        }

    }


//    }
}
