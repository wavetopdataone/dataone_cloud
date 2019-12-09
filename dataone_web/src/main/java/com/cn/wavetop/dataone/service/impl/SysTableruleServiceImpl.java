package com.cn.wavetop.dataone.service.impl;

import com.cn.wavetop.dataone.dao.*;
import com.cn.wavetop.dataone.entity.*;
import com.cn.wavetop.dataone.entity.vo.ToData;
import com.cn.wavetop.dataone.entity.vo.ToDataMessage;
import com.cn.wavetop.dataone.service.SysTableruleService;
import com.cn.wavetop.dataone.util.DBConn;
import com.cn.wavetop.dataone.util.DBConns;
import com.cn.wavetop.dataone.util.DBHelper;
import com.cn.wavetop.dataone.util.PermissionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.transaction.Transactional;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class SysTableruleServiceImpl implements SysTableruleService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SysTableruleRepository sysTableruleRepository;
    @Autowired
    private SysJobrelaRepository sysJobrelaRepository;
    @Autowired
    private SysFieldruleRepository sysFieldruleRepository;
    @Autowired
    private SysDbinfoRespository sysDbinfoRespository;
    @Autowired
    private SysFilterTableRepository sysFilterTableRepository;
    @Autowired
    private SysJobrelaRelatedRespository sysJobrelaRelatedRespository;
    @Override
    public Object tableruleAll() {
        List<SysTablerule> sysUserList=sysTableruleRepository.findAll();
        return ToData.builder().status("1").data(sysUserList).build();
    }

    @Override
    public Object checkTablerule(long job_id) {
        List<SysTablerule> sysUserList=sysTableruleRepository.findByJobId(job_id);
        //查询过滤表
//        List<SysFilterTable> sysUserList= sysFilterTableRepository.findJobId(job_id);
        String sql="";
        SysDbinfo sysDbinfo=new SysDbinfo();
        List<SysTablerule> list=new ArrayList<SysTablerule>();
        List<String> stringList=new ArrayList<String>();
        StringBuffer stringBuffer=new StringBuffer("");
        SysTablerule tablerule=new SysTablerule();
        //查詢关联的数据库连接表jobrela
        List<SysJobrela> sysJobrelaList=sysJobrelaRepository.findById(job_id);
        //查询到数据库连接
        if(sysJobrelaList!=null&&sysJobrelaList.size()>0) {
             sysDbinfo = sysDbinfoRespository.findById(sysJobrelaList.get(0).getSourceId().longValue());
        }else{
            return ToDataMessage.builder().status("0").message("该任务没有连接").build();
        }
        if(sysDbinfo.getType()==2){
            //mysql
            sql = "show tables";
        }else if(sysDbinfo.getType()==1){
            //oracle
            sql = "SELECT TABLE_NAME FROM DBA_ALL_TABLES WHERE OWNER='" + sysDbinfo.getSchema() + "'AND TEMPORARY='N' AND NESTED='NO'";
        }

        if(sysUserList!=null&&sysUserList.size()>0){
            for(SysTablerule sysTablerule:sysUserList){
                stringBuffer.append(sysTablerule.getSourceTable());
                stringBuffer.append(",");
            }
//            for(SysFilterTable sysTablerule:sysUserList){
//                stringBuffer.append(sysTablerule.getFilterTable());
//                stringBuffer.append(",");
//            }
            tablerule.setSourceTable(stringBuffer.toString());
            stringList = DBConns.getConn(sysDbinfo, tablerule, sql);

            tablerule=new SysTablerule();
        }else{
            stringList = DBConns.getConn(sysDbinfo, tablerule, sql);
        }
        if(sysJobrelaList!=null&&sysJobrelaList.size()>0) {
            sysJobrelaList.get(0).setJobStatus("0");
            SysJobrela sysJobrela = sysJobrelaRepository.save(sysJobrelaList.get(0));
        }

        //修改子任务状态
        Optional<SysJobrela> ss=null;
        List<SysJobrelaRelated> sysJobrelaRelateds= sysJobrelaRelatedRespository.findByMasterJobId(Long.valueOf(job_id));
        if(sysJobrelaRelateds!=null&&sysJobrelaRelateds.size()>0) {
        for(SysJobrelaRelated sysJobrelaRelated:sysJobrelaRelateds){
            ss=sysJobrelaRepository.findById(sysJobrelaRelated.getSlaveJobId());
              if(ss!=null){
                  ss.get().setJobStatus("0");
                  sysJobrelaRepository.save(ss.get());
              }
        }
        }


        for(String s:stringList){
            list.add(SysTablerule.builder().sourceTable(s).build());
        }
        return ToData.builder().status("1").data(list).build();

    }
    @Transactional
    @Override
    public Object addTablerule(SysTablerule sysTablerule) {
        try{
            System.out.println(sysTablerule+"-----------"+sysTablerule.getId());
            List<SysTablerule> sysTableruleList =sysTableruleRepository.findByJobId(sysTablerule.getJobId());
            String[] b= sysTablerule.getSourceTable().split(",");
            List<SysTablerule> list=new ArrayList<SysTablerule>();
            String[]c=null;
            if(sysTableruleList!=null&&sysTableruleList.size()>0){
                return ToDataMessage.builder().status("0").message("任务已存在").build();
            }else{
                String destTable=sysTablerule.getDestTable();
                if(destTable!=null&&!"".equals(destTable)){
                    c= sysTablerule.getDestTable().split(",");
                }
                SysTablerule sysTablerule1=null;
                for(int i=0;i<b.length;i++){
                    if(c!=null&&c.length>0){
                        sysTablerule.setDestTable(c[i]);
                    }else{
                        destTable=b[i];
                        sysTablerule.setDestTable(destTable);
                    }
                    sysTablerule.setSourceTable(b[i]);
                    sysTablerule1= sysTableruleRepository.save(sysTablerule);
                    list.add(sysTablerule1);
                }
                return ToData.builder().status("1").data(list).build();
            }
        }catch (Exception e){
            StackTraceElement stackTraceElement = e.getStackTrace()[0];
            logger.error("*"+stackTraceElement.getLineNumber()+e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }

    }
    @Transactional
    @Override
    public Object editTablerule(SysTablerule sysTablerule) {
        try{
            System.out.println(sysTablerule+"-----------"+sysTablerule.getId());
            List<SysTablerule> sysTableruleList =sysTableruleRepository.findByJobId(sysTablerule.getJobId());
            String[] b= sysTablerule.getSourceTable().split(",");
            List<SysTablerule> list=new ArrayList<SysTablerule>();
            List<String> stringList=new ArrayList<String>();
            String sql="";
            //查詢关联的数据库连接表jobrela
            List<SysJobrela> sysJobrelaList=sysJobrelaRepository.findById(sysTablerule.getJobId().longValue());
            //查询到数据库连接
            SysDbinfo sysDbinfo=sysDbinfoRespository.findById(sysJobrelaList.get(0).getSourceId().longValue());
            SysTablerule sysTablerule2=null;
            if(sysDbinfo.getType()==2){
                //mysql
                sql = "show tables";
            }else if(sysDbinfo.getType()==1){
                //oracle
                sql = "SELECT TABLE_NAME FROM DBA_ALL_TABLES WHERE OWNER='" + sysDbinfo.getSchema() + "'AND TEMPORARY='N' AND NESTED='NO'";
            }
            if(sysDbinfo.getSourDest()==0) {
                //去过重复的数据
                stringList = DBConns.getConn(sysDbinfo, sysTablerule, sql);
            }else{
                return ToData.builder().status("0").message("该连接是目标端").build();
            }
            //判断是否有子任务
           List<SysJobrelaRelated> sysJobrelaRelateds= sysJobrelaRelatedRespository.findByMasterJobId(sysTablerule.getJobId());
            if(sysTableruleList!=null&&sysTableruleList.size()>0){
                //若是修改主任务则先删除标规则字段规则
                int a= sysTableruleRepository.deleteByJobId(sysTablerule.getJobId());
                sysFilterTableRepository.deleteByJobId(sysTablerule.getJobId());
                if(PermissionUtils.isPermitted("3")) {
                    if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                        //子任务的删除规则
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                            sysTableruleRepository.deleteByJobId(sysJobrelaRelated.getSlaveJobId());
                            sysFilterTableRepository.deleteByJobId(sysJobrelaRelated.getSlaveJobId());
                        }
                    }
                }
                System.out.println(a);
                SysTablerule sysTablerule1=null;
                SysFilterTable sysFilterTable=null;
                //添加过滤的表
                for(int i=0;i<stringList.size();i++){
                    sysFilterTable=new SysFilterTable();
                    sysTablerule2=new SysTablerule();
                    sysTablerule2.setJobId(sysTablerule.getJobId());
                    sysTablerule2.setSourceTable(stringList.get(i));
                    sysTablerule2.setVarFlag(Long.valueOf(1));
                    sysTablerule1= sysTableruleRepository.save(sysTablerule2);
                    sysFilterTable.setJobId(sysTablerule.getJobId());
                    sysFilterTable.setFilterTable(stringList.get(i));
                    sysFilterTableRepository.save(sysFilterTable);
                    //添加子任务过滤的表
                    if(sysJobrelaRelateds!=null&&sysJobrelaRelateds.size()>0) {
                        SysFilterTable sysFilterTable3=null;
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                            sysFilterTable3=new SysFilterTable();
                            sysFilterTable3.setJobId(sysJobrelaRelated.getSlaveJobId());
                            sysFilterTable3.setFilterTable(stringList.get(i));
                            sysFilterTableRepository.save(sysFilterTable3);
                        }
                    }
//                    list.add(sysTablerule1);
                }

                    return ToData.builder().status("1").message("修改成功").build();

            }else{
                //添加
                SysTablerule sysTablerule1=null;
                SysFilterTable sysFilterTable=null;
                //添加过滤的表
                for(int i=0;i<stringList.size();i++){
                    sysTablerule2=new SysTablerule();
                    sysFilterTable=new SysFilterTable();
                    sysTablerule2.setJobId(sysTablerule.getJobId());
                    sysTablerule2.setSourceTable(stringList.get(i));
                    sysTablerule2.setVarFlag(Long.valueOf(1));
                    sysTablerule1= sysTableruleRepository.save(sysTablerule2);
                    sysFilterTable.setJobId(sysTablerule.getJobId());
                    sysFilterTable.setFilterTable(stringList.get(i));
                    sysFilterTableRepository.save(sysFilterTable);
                    //添加子任务过滤的表
                    if(sysJobrelaRelateds!=null&&sysJobrelaRelateds.size()>0) {
                        SysFilterTable sysFilterTable3=null;

                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
//                            SysFilterTable sysFilterTable1= sysFilterTableRepository.findByJobId(sysJobrelaRelated.getSlaveJobId());
//                            if(sysFilterTable1!=null){
//                                continue;
//                            }else {
                            sysFilterTable3=new SysFilterTable();
                            sysFilterTable3.setJobId(sysJobrelaRelated.getSlaveJobId());
                            sysFilterTable3.setFilterTable(stringList.get(i));
                                sysFilterTableRepository.save(sysFilterTable3);
//                            }
                        }
                    }
//                    list.add(sysTablerule1);
                }

                    return ToData.builder().status("1").message("新增成功").build();

            }
        }catch (Exception e){
            StackTraceElement stackTraceElement = e.getStackTrace()[0];
            logger.error("*"+stackTraceElement.getLineNumber()+e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }

    }

    @Transactional
    @Override
    public Object deleteTablerule(long job_id) {
        try{
            List<SysTablerule> sysTableruleList=sysTableruleRepository.findByJobId(job_id);
            List<SysFieldrule> sysFieldruleList=sysFieldruleRepository.findByJobId(job_id);
            if(sysTableruleList!=null&&sysTableruleList.size()>0){
                int result= sysTableruleRepository.deleteByJobId(job_id);
                if(sysFieldruleList!=null&&sysFieldruleList.size()>0) {
                    int result2 = sysFieldruleRepository.deleteByJobId(job_id);
                }
                return ToDataMessage.builder().status("1").message("删除成功").build();

            }else{
                return ToDataMessage.builder().status("0").message("任务不存在").build();
            }
        }catch (Exception e){
            StackTraceElement stackTraceElement = e.getStackTrace()[0];
            logger.error("*"+stackTraceElement.getLineNumber()+e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return ToDataMessage.builder().status("0").message("发生错误").build();
        }

    }

    @Override
    public Object linkDataTable(SysDbinfo sysDbinfo) {
        String sql = "";
        List<Object> list = new ArrayList<Object>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        //ArrayList<Object> data = new ArrayList<>();
        if (sysDbinfo.getType()==2) {
            sql = "show tables";
            try {
                conn = DBConns.getMySQLConn(sysDbinfo);
                stmt = conn.prepareStatement(sql);
                rs = stmt.executeQuery(sql);
                String tableName = null;
                while (rs.next()) {
                    tableName = rs.getString(1);
                    list.add(tableName);
                    System.out.println(tableName);
                }//显示数据
                return ToData.builder().status("1").data(list).build();
            } catch (SQLException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                e.printStackTrace();
                StackTraceElement stackTraceElement = e.getStackTrace()[0];
                logger.error("*"+stackTraceElement.getLineNumber()+e);
                return ToDataMessage.builder().status("0").message("数据库连接错误").build();

            }finally {
                DBConns.close(stmt,conn,rs);
            }
        } else if (sysDbinfo.getType()==1) {
            sql = "SELECT TABLE_NAME FROM DBA_ALL_TABLES WHERE OWNER='" + sysDbinfo.getSchema() + "'AND TEMPORARY='N' AND NESTED='NO'";
            String tableName = "";
            try {
                conn = DBConns.getOracleConn(sysDbinfo);
                stmt = conn.prepareStatement(sql);
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    tableName = rs.getString(1);
                    System.out.println(tableName);
                    list.add(tableName);
                }
                return ToData.builder().status("1").data(list).build();
            } catch (SQLException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                e.printStackTrace();
                StackTraceElement stackTraceElement = e.getStackTrace()[0];
                logger.error("*"+stackTraceElement.getLineNumber()+e);
                return ToDataMessage.builder().status("0").message("数据库连接错误").build();
            }finally {
                DBConns.close(stmt,conn,rs);
            }

        } else {
            return ToDataMessage.builder().status("0").message("类型不正确").build();
        }


    }




}
