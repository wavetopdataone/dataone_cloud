package com.cn.wavetop.dataone.controller;

import com.cn.wavetop.dataone.dao.*;
import com.cn.wavetop.dataone.entity.*;
import com.cn.wavetop.dataone.service.SysJobinfoService;
import com.cn.wavetop.dataone.service.SysJobrelaService;
import com.cn.wavetop.dataone.service.SysMonitoringService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/toback")
public class ToBackController {

    @Autowired
    private SysJobrelaService sysJobrelaService;

    @Autowired
    private SysJobinfoService sysJobinfoService;
    @Autowired
    private SysMonitoringService sysMonitoringService;
    @Autowired
    private  SysJobrelaRespository sysJobrelaRespository;
    @Autowired
    private SysDbinfoRespository sysDbinfoRespository;
    @Autowired
    private SysMonitoringRepository sysMonitoringRepository;
    @Autowired
    private ErrorLogRespository errorLogRespository;

    @Autowired
    private KafkaDestFieldRepository kafkaDestFieldRepository;
    @Autowired
    private KafkaDestTableRepository kafkaDestTableRepository;
    /**
     * 根据jobid查询数据信息
     * @param jobId
     * @return
     */
    @ApiOperation(value = "后台查询", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "后台查询")
    @GetMapping("/findById/{jobId}")
    public Object findDbinfoById( @PathVariable Long jobId) {

        return sysJobrelaService.findDbinfoById(jobId);
    }

    /**
     * 查询同步数据类型
     * @param Id
     * @return
     */
    @ApiOperation(value = "查看同步方式", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "查看同步方式")
    @GetMapping("/find_range/{Id}")
    public Object findRangeByJobId(@PathVariable long Id) {
        return sysJobrelaService.findRangeByJobId(Id);
    }
    /**
     * 更新读监听数据
     * @param Id
     * @param readData
     */
    @ApiOperation(value = "更新监听数据", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "更新监听数据")
    @GetMapping("/readmonitoring/{Id}")
    public void updateReadMonitoring(@PathVariable long Id, @RequestParam Long readData,String table){
        sysMonitoringService.updateReadMonitoring(Id,readData,table);
    }


    /**
     * 更新写监听数据
     * @param Id
     * @param writeData
     */
    @ApiOperation(value = "更新监听数据", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "更新监听数据")
    @GetMapping("/writemonitoring/{Id}")
    public void updateWriteMonitoring(@PathVariable long Id,@RequestParam Long writeData,String table){
        sysMonitoringService.updateWriteMonitoring(Id,writeData,table);
    }

    /**
     * 更新写监听数据
     * @param Id
     * @param
     */
    @ApiOperation(value = "查询监控目的表", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "查询监控目的表")
    @GetMapping("/find_destTable/{Id}")
    public Object monitoringTable(@PathVariable long Id){
        String str="";
        String dbName="";
        List<String> list=new ArrayList<>();
       SysJobrela sysJobrela= sysJobrelaRespository.findById(Id);
      Optional<SysDbinfo> sysDbinfo= sysDbinfoRespository.findById(sysJobrela.getDestId());
      if(sysDbinfo.get().getType()==1){
          dbName=sysDbinfo.get().getSchema();
      }else if(sysDbinfo.get().getType()==2){
          dbName=sysDbinfo.get().getDbname();
      }
        List<SysMonitoring> sysMonitoringList=sysMonitoringRepository.findByJobId(Id);
        if(sysMonitoringList!=null&&sysMonitoringList.size()>0){
            for(SysMonitoring sysMonitoring:sysMonitoringList){
               if(!"".equals(dbName)&&dbName!=null){
                   str=dbName+"."+sysMonitoring.getDestTable();
                   list.add(str);
               }else{
                   list.add(sysMonitoring.getDestTable());
               }
            }
        }
       return list;
    }
    /**
     * 更新写监听数据
     * @para
     * @param
     */
    @ApiOperation(value = "读取速率", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "插入读取速率")
    @GetMapping("/updateReadRate/{readRate}")
    public void monitoringTable(@PathVariable Long readRate,Long jobId) {
        //todo 后面要分表
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(jobId);
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            for (SysMonitoring sysMonitoring : sysMonitoringList) {
                sysMonitoring.setReadRate(readRate);
                sysMonitoringRepository.save(sysMonitoring);
            }
        }
    }

    /**
     * 更新写监听数据
     * @para
     * @param
     */
    @ApiOperation(value = "写入速率", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "插入读取速率")
    @GetMapping("/updateDisposeRateAndError/{jobId}")
    public void updateDisposeRateAndError(@PathVariable Long  jobId,Long disposeRate,Long errorData) {
        //todo 后面要分表
        List<SysMonitoring> sysMonitoringList = sysMonitoringRepository.findByJobId(jobId);
        if (sysMonitoringList != null && sysMonitoringList.size() > 0) {
            for (SysMonitoring sysMonitoring : sysMonitoringList) {
                sysMonitoring.setDisposeRate(disposeRate);
                sysMonitoring.setErrorData(errorData);
                sysMonitoringRepository.save(sysMonitoring);
            }
        }
    }
    /**
     * 错误队列
     * @para
     * @param
     */
    @ApiOperation(value = "错误队列", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "插入读取速率")
    @GetMapping("/InsertLogError/{jobId}")
    public void InsertLogError(@PathVariable Long jobId,String optContext,String content) {
        //todo 后面要分表
         ErrorLog errorLog=new ErrorLog();
         errorLog.setContent(content);
         errorLog.setOptContext(optContext);
         errorLog.setJobId(jobId);
         errorLog.setOptTime(new Date());
         errorLogRespository.save(errorLog);

    }
    /**
     * kafka需要的同步数据
     */
    @ApiOperation(value = "kafka需要的同步数据", httpMethod = "GET", protocols = "HTTP", produces = "application/json", notes = "kafka需要的同步数据")
    @GetMapping("/kafkaFiled/{jobId}")
    public Object kafkaFiled(@PathVariable Long jobId){
       List<KafkaDestTable> kafkaDestTableList= kafkaDestTableRepository.findByJobId(jobId);
       List<Object> list=new ArrayList<>();
        Map<String,Object> map=new HashMap<>();
       for(KafkaDestTable kafkaDestTable:kafkaDestTableList){
          List<KafkaDestField> kafkaDestFieldList= kafkaDestFieldRepository.findByKafkaDestId(kafkaDestTable.getId());
           list=new ArrayList<>();
           list.add(kafkaDestFieldList);
           map.put(kafkaDestTable.getDestTable(),list);
       }
       return map;
    }
}
