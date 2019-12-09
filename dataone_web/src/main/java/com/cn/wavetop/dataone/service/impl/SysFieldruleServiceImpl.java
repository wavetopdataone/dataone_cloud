package com.cn.wavetop.dataone.service.impl;

import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.cn.wavetop.dataone.dao.*;
import com.cn.wavetop.dataone.entity.*;
import com.cn.wavetop.dataone.entity.vo.ToData;
import com.cn.wavetop.dataone.entity.vo.ToDataMessage;
import com.cn.wavetop.dataone.service.SysFieldruleService;
import com.cn.wavetop.dataone.util.DBConn;
import com.cn.wavetop.dataone.util.DBConns;
import com.cn.wavetop.dataone.util.DBHelper;
import com.cn.wavetop.dataone.util.PermissionUtils;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import javax.xml.crypto.Data;
import java.sql.*;
import java.util.*;


/**
 * @Author yongz
 * @Date 2019/10/11、15:27
 */
@Service
public class SysFieldruleServiceImpl implements SysFieldruleService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SysFieldruleRespository repository;
    @Autowired
    private SysJobrelaRespository sysJobrelaRespository;
    @Autowired
    private SysTableruleRespository sysTableruleRespository;
    @Autowired
    private SysJobrelaRepository sysJobrelaRepository;
    @Autowired
    private SysDbinfoRespository sysDbinfoRespository;
    @Autowired
    private SysFieldruleRepository sysFieldruleRepository;
    @Autowired
    private SysFilterTableRepository sysFilterTableRepository;
    @Autowired
    private SysJobrelaRelatedRespository sysJobrelaRelatedRespository;
    @Autowired
    private SysDesensitizationRepository sysDesensitizationRepository;
    @Autowired
    private SysFiledTypeRepository sysFiledTypeRepository;
    @Autowired
    private KafkaDestFieldRepository kafkaDestFieldRepository;
    @Autowired
    private KafkaDestTableRepository kafkaDestTableRepository;
    @Override
    public Object getFieldruleAll() {
        return ToData.builder().status("1").data(repository.findAll()).build();
    }

    @Override
    public Object checkFieldruleByJobId(long job_id) {
        if (repository.existsByJobId(job_id)) {
            List<SysFieldrule> sysFieldrule = repository.findByJobId(job_id);
            Map<Object, Object> map = new HashMap();
            map.put("status", 1);
            map.put("data", sysFieldrule);
            return map;
        } else {
            return ToData.builder().status("0").message("任务不存在").build();

        }
    }

    @Override
    public Object addFieldrule(SysFieldrule sysFieldrule) {
        return "该接口已弃用";
    }

    //判断字段是否发生改变
    public boolean fieldChange(String[] split) {
        boolean flag = false;
        for (String s : split) {
            String[] ziduan = s.split(",");
            //若字段改变则存入字段规则表
//                if (!ziduan[0].equals(ziduan[1]) || !ziduan[3].equals(ziduan[7]) || !ziduan[5].equals(ziduan[8])) {
            if (!ziduan[0].equals(ziduan[1])) {
                flag = true;
                return flag;
            }
        }
        return flag;
    }

    @Transactional
    @Override
    public Object editFieldrule(String list_data, String source_name, String dest_name, Long job_id) {
        System.out.println(list_data+"-----------");
        Map<Object, Object> map = new HashMap();
        SysFieldrule sysFieldrule1 = new SysFieldrule();
        List<SysFieldrule> sysFieldrules = new ArrayList<>();
        List<SysFieldrule> list = new ArrayList<>();
        String sql = "";
        String[] split;
        try {
            if (list_data != null && source_name != null && dest_name != null && !"undefined".equals(list_data) && !"undefined".equals(dest_name)) {

                split = list_data.replace("$", "@").split(",@,");

                SysTablerule byJobIdAndSourceTable = new SysTablerule();
                SysTablerule byJobIdAndSourceTable2 = null;

                SysDbinfo sysDbinfo = new SysDbinfo();//源端
                SysDbinfo sysDbinfo2 = new SysDbinfo();//目标端
                //查詢关联的数据库连接表jobrela
                List<SysJobrela> sysJobrelaList = sysJobrelaRepository.findById(job_id.longValue());
                //查询到数据库连接
                if (sysJobrelaList != null && sysJobrelaList.size() > 0) {
                    sysDbinfo = sysDbinfoRespository.findById(sysJobrelaList.get(0).getSourceId().longValue());
                    sysDbinfo2 = sysDbinfoRespository.findById(sysJobrelaList.get(0).getDestId().longValue());
                } else {
                    return ToDataMessage.builder().status("0").message("该任务没有连接").build();
                }
                //若目标端存在此表则提示用户
                String sqlss = "";
                if (sysDbinfo2.getType() == 1) {
                    //oracle
                    sqlss = "SELECT TABLE_NAME FROM DBA_ALL_TABLES WHERE OWNER='" + sysDbinfo2.getSchema() + "'AND TEMPORARY='N' AND NESTED='NO'";
                } else if (sysDbinfo2.getType() == 2) {
                    //mysql
                    sqlss = "show tables";
                } else if (sysDbinfo2.getType() == 3) {
                    //sqlserver
                    sqlss = "select name from sysobjects where xtype='u'";
                }
                System.out.print(sysDbinfo2 + "-------------");
                //查询目标端是否出现此表
                List<String> tablename = DBConns.existsTableName(sysDbinfo2, sqlss, source_name, dest_name);
                String sssql = "";
                if (sysDbinfo.getType() == 1) {
                    //oracle
                    sssql = "SELECT TABLE_NAME FROM DBA_ALL_TABLES WHERE OWNER='" + sysDbinfo2.getSchema() + "'AND TEMPORARY='N' AND NESTED='NO'";
                } else if (sysDbinfo.getType() == 2) {
                    //mysql
                    sssql = "show tables";
                }
                //查詢源端是否存在此表名
                List<String> sourcetablename = DBConns.existsTableName(sysDbinfo, sssql, source_name, dest_name);
                //查看目的端是否存在表名
                if (tablename != null && tablename.size() > 0) {
                    return ToDataMessage.builder().status("0").message("目的端表名" + dest_name + "已经存在").build();
                }
                //查看源端是否存在表名
                if (sourcetablename != null && sourcetablename.size() > 0) {
                    return ToDataMessage.builder().status("0").message("源端表名" + dest_name + "已经存在").build();
                }
                //查询是否关联的有子任务
                List<SysJobrelaRelated> sysJobrelaRelateds = sysJobrelaRelatedRespository.findByMasterJobId(job_id);
                int a = sysTableruleRespository.deleteByJobIdAndSourceTable(job_id, source_name);
                if (PermissionUtils.isPermitted("3")) {
                    //查询该任务有没有关联的子任务
                    if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                            //先删除表规则字段規則過濾规则
                            sysTableruleRespository.deleteByJobIdAndSourceTable(sysJobrelaRelated.getSlaveJobId(), source_name);
                            sysFieldruleRepository.deleteByJobIdAndSourceName(sysJobrelaRelated.getSlaveJobId(), source_name);
                            sysFilterTableRepository.deleteByJobIdAndFilterTable(sysJobrelaRelated.getSlaveJobId(), source_name);
                            kafkaDestFieldRepository.deleteByJobId(sysJobrelaRelated.getSlaveJobId());
                            kafkaDestTableRepository.deleteByJobId(sysJobrelaRelated.getSlaveJobId());
                        }
                    }
                }
                //删除kafka主任务的标规则
                kafkaDestTableRepository.deleteByJobId(job_id);
                KafkaDestTable kafkaDestTable = new KafkaDestTable();
                kafkaDestTable.setJobId(job_id);
                System.out.println(sysDbinfo2.getId().intValue()+"-----------------");
                kafkaDestTable.setDestDbid(sysDbinfo2.getId());
                kafkaDestTable.setDestTable(dest_name);
                System.out.println(kafkaDestTable+"----------------");
                kafkaDestTableRepository.save(kafkaDestTable);
                if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                    KafkaDestTable kafkaDestTable1 = null;
                    for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                        kafkaDestTable1 = new KafkaDestTable();
                        kafkaDestTable1.setJobId(sysJobrelaRelated.getSlaveJobId());
                        kafkaDestTable1.setDestDbid(sysDbinfo2.getId());
                        kafkaDestTable1.setDestTable(dest_name);
                        System.out.println(kafkaDestTable1+"----------------");
                        kafkaDestTableRepository.save(kafkaDestTable1);
                    }
                }
                //删除主任务的表规则
                sysTableruleRespository.deleteByJobIdAndSourceTable(job_id, source_name);
                //若源端表和目标端不一致则添加表规则
                if (!source_name.equals(dest_name)) {
                    byJobIdAndSourceTable.setDestTable(dest_name);
                    byJobIdAndSourceTable.setJobId(job_id);
                    byJobIdAndSourceTable.setSourceTable(source_name);
                    byJobIdAndSourceTable.setVarFlag(Long.valueOf(2));//2代表映射
                    System.out.println(byJobIdAndSourceTable+"----------------");

                    sysTableruleRespository.save(byJobIdAndSourceTable);
                    //若有子任务为子任务添加表规则
                    if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
//                        SysTablerule sysTablerule= sysTableruleRespository.findByJobId(sysJobrelaRelated.getSlaveJobId());
//                        if(sysTablerule!=null){
//                            continue;
//                        }else {
                            byJobIdAndSourceTable2 = new SysTablerule();
                            byJobIdAndSourceTable2.setDestTable(dest_name);
                            byJobIdAndSourceTable2.setJobId(sysJobrelaRelated.getSlaveJobId());
                            byJobIdAndSourceTable2.setSourceTable(source_name);
                            byJobIdAndSourceTable2.setVarFlag(Long.valueOf(2));
                            sysTableruleRespository.save(byJobIdAndSourceTable2);
//                        }
                        }
                    }
                } else {
                    //表相同但是字段发生映射改变，表也要存入tablerule
                    boolean flag = fieldChange(split);
                    if (flag) {
                        byJobIdAndSourceTable.setDestTable(dest_name);
                        byJobIdAndSourceTable.setJobId(job_id);
                        byJobIdAndSourceTable.setSourceTable(source_name);
                        byJobIdAndSourceTable.setVarFlag(Long.valueOf(2));//2代表映射
                        sysTableruleRespository.save(byJobIdAndSourceTable);
                        //若有子任务为子任务添加表规则
                        if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                            for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
//                        SysTablerule sysTablerule= sysTableruleRespository.findByJobId(sysJobrelaRelated.getSlaveJobId());
//                        if(sysTablerule!=null){
//                            continue;
//                        }else {
                                byJobIdAndSourceTable2 = new SysTablerule();
                                byJobIdAndSourceTable2.setDestTable(dest_name);
                                byJobIdAndSourceTable2.setJobId(sysJobrelaRelated.getSlaveJobId());
                                byJobIdAndSourceTable2.setSourceTable(source_name);
                                byJobIdAndSourceTable2.setVarFlag(Long.valueOf(2));
                                sysTableruleRespository.save(byJobIdAndSourceTable2);
//                        }
                            }
                        }
                    }
                }
                //删除主任务字段规则
                sysFieldruleRepository.deleteByJobIdAndSourceName(job_id, source_name);
                //删除kafka字段规则
                kafkaDestFieldRepository.deleteByJobId(job_id);
                //查询kafka需要的表名称
                KafkaDestTable kafkaDestTable2 = kafkaDestTableRepository.findByJobIdAndDestTable(job_id,dest_name);
                KafkaDestField kafkaDestField=null;
                for (String s : split) {
                    String[] ziduan = s.split(",");
                    System.out.println(s+"xieuxizi");
                    System.out.println(ziduan[5]+"-----------");

                    //添加kafka字段规则
                    //todo 暂时使用源端的，后续要换目标端
                     kafkaDestField = new KafkaDestField();
                    kafkaDestField.setJobId(job_id);
                    System.out.println(ziduan[5]+"----------------");
                    if(ziduan[5]==null||"".equals(ziduan[5])){
                        ziduan[5]="0";
                    }
                    kafkaDestField.setFieldLength(Double.valueOf(ziduan[5]));
                    kafkaDestField.setFieldName(ziduan[1]);
                    kafkaDestField.setFieldNotnull(ziduan[4]);
                    kafkaDestField.setFieldType(ziduan[2]);
                    kafkaDestField.setKafkaDestId(kafkaDestTable2.getId());
                    System.out.println(kafkaDestField+"--------------");
                    KafkaDestField kafkaDestField2= kafkaDestFieldRepository.save(kafkaDestField);
                    System.out.println(kafkaDestField2+"--------------");

                    System.out.println(sysJobrelaRelateds+"--------------");

                    if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                        KafkaDestField kafkaDestField1 = null;
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                            kafkaDestField1 = new KafkaDestField();
                            kafkaDestField1.setJobId(sysJobrelaRelated.getSlaveJobId());
                            kafkaDestField1.setFieldLength(Double.valueOf(ziduan[5]));
                            kafkaDestField1.setFieldName(ziduan[1]);
                            kafkaDestField1.setFieldNotnull(ziduan[4]);
                            kafkaDestField1.setFieldType(ziduan[2]);
                            kafkaDestField1.setKafkaDestId(kafkaDestTable2.getId());
                            kafkaDestFieldRepository.save(kafkaDestField1);
                        }
                    }
                    System.out.println("xuezihao--------------------");

                    //若字段改变则存入字段规则表
//                if (!ziduan[0].equals(ziduan[1]) || !ziduan[3].equals(ziduan[7]) || !ziduan[5].equals(ziduan[8])) {
                    if (!ziduan[0].equals(ziduan[1])) {
                        SysFieldrule build = SysFieldrule.builder().fieldName(ziduan[0])
                                .destFieldName(ziduan[1])
                                .jobId(job_id)
                                .type(ziduan[2])
                                .scale(ziduan[3])
                                .notNull(Long.valueOf(ziduan[4]))
                                .accuracy(ziduan[5])
                                .sourceName(source_name)
                                .destName(dest_name).varFlag(Long.valueOf(2)).build();
                        System.out.println(build+"xuezihao--------------------");

                        sysFieldrules.add(repository.save(build));

                        //若有子任务也保存规则
                        if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                            for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
//                            List<SysFieldrule> sysFieldrule= repository.findByJobId(sysJobrelaRelated.getSlaveJobId());
//                            if(sysFieldrule!=null&&sysFieldrule.size()>0){
//                                continue;
//                            }else {
                                SysFieldrule builds = SysFieldrule.builder().fieldName(ziduan[0])
                                        .destFieldName(ziduan[1])
                                        .jobId(sysJobrelaRelated.getSlaveJobId())
                                        .type(ziduan[2])
                                        .scale(ziduan[3])
                                        .notNull(Long.valueOf(ziduan[4]))
                                        .accuracy(ziduan[5])
                                        .sourceName(source_name)
                                        .destName(dest_name).varFlag(Long.valueOf(2)).build();
                                sysFieldrules.add(repository.save(builds));
//                            }
                            }
                        }

                    }
                    //字段类型发生改变
                    //查询字段的默认映射
//              List<SysFiledType> sysFiledTypeList=sysFiledTypeRepository.findBySourceTypeAndDestTypeAndSourceFiledType(String.valueOf(sysDbinfo.getType()),String.valueOf(sysDbinfo2.getType()),ziduan[2]);
//                if(sysFiledTypeList!=null&&sysFiledTypeList.size()>0){
//                    //有可能是多个，因为数据库有根据长度不同字段对应不同，但是是少数，先留着
////                    for(int i=0;i<sysFiledTypeList.size();i=++)
//                    //判断目标端字段是否是默认映射的字段
//                      if(!sysFiledTypeList.get(0).getDestFiledType().equals(ziduan[6])){
//                          SysFieldrule build = SysFieldrule.builder().fieldName(ziduan[0])
//                                  .destFieldName(ziduan[1])
//                                  .jobId(job_id)
//                                  .type(ziduan[2])
//                                  .scale(ziduan[3])
//                                  .notNull(Long.valueOf(ziduan[4]))
//                                  .accuracy(ziduan[5])
//                                  .sourceName(source_name)
//                                  .destName(dest_name).varFlag(Long.valueOf(2)).build();
//                          sysFieldrules.add(repository.save(build));
//
//                }
//                      //如果有子任务也需要添加
//                    if(sysJobrelaRelateds!=null&&sysJobrelaRelateds.size()>0) {
//                        for(SysJobrelaRelated sysJobrelaRelated:sysJobrelaRelateds) {
////                            List<SysFieldrule> sysFieldrule= repository.findByJobId(sysJobrelaRelated.getSlaveJobId());
////                            if(sysFieldrule!=null&&sysFieldrule.size()>0){
////                                continue;
////                            }else {
//                            SysFieldrule builds = SysFieldrule.builder().fieldName(ziduan[0])
//                                    .destFieldName(ziduan[1])
//                                    .jobId(sysJobrelaRelated.getSlaveJobId())
//                                    .type(ziduan[2])
//                                    .scale(ziduan[3])
//                                    .notNull(Long.valueOf(ziduan[4]))
//                                    .accuracy(ziduan[5])
//                                    .sourceName(source_name)
//                                    .destName(dest_name).varFlag(Long.valueOf(2)).build();
//                            sysFieldrules.add(repository.save(builds));
////                            }
//                        }
//                    }
//                }
                }
                if (sysDbinfo.getType() == 2) {
                    sql = "select Column_Name as ColumnName,data_type as TypeName, (case when data_type = 'float' or data_type = 'int' or data_type = 'datetime' or data_type = 'bigint' or data_type = 'double' or data_type = 'decimal' then NUMERIC_PRECISION else CHARACTER_MAXIMUM_LENGTH end ) as length, NUMERIC_SCALE as Scale,(case when IS_NULLABLE = 'YES' then 0 else 1 end) as CanNull  from information_schema.columns where table_schema ='" + sysDbinfo.getDbname() + "' and table_name='" + source_name + "'";
                } else if (sysDbinfo.getType() == 1) {
                    sql = "SELECT COLUMN_NAME, DATA_TYPE, NVL(DATA_LENGTH,0), NVL(DATA_PRECISION,0), NVL(DATA_SCALE,0), NULLABLE, COLUMN_ID ,DATA_TYPE_OWNER FROM DBA_TAB_COLUMNS WHERE TABLE_NAME='" + source_name
                            + "' AND OWNER='" + sysDbinfo.getSchema() + "'";
                }
                //不同步的字段
                list = DBConns.getResult(sysDbinfo, sql, list_data);
                SysFilterTable sysFilterTable = null;
                sysFilterTableRepository.deleteByJobIdAndFilterTable(job_id, source_name);
                System.out.println("xuezihaoxuezihaoxuezihao" + list.size() + "---------------");
                for (SysFieldrule sysFieldrule : list) {
                    sysFilterTable = new SysFilterTable();
//                sysFieldrule1 = new SysFieldrule();
//                sysFieldrule1.setFieldName(sysFieldrule.getFieldName());
//                sysFieldrule1.setScale(sysFieldrule.getScale());
//                sysFieldrule1.setType(sysFieldrule.getType());
//                sysFieldrule1.setDestFieldName(sysFieldrule.getFieldName());
//                sysFieldrule1.setJobId(job_id);
//                sysFieldrule1.setSourceName(source_name);
//                sysFieldrule1.setDestName(dest_name);
//                sysFieldrule1.setVarFlag(Long.valueOf(1));
//                SysFieldrule sysFieldrule2 = sysFieldruleRepository.save(sysFieldrule1);
                    sysFilterTable.setFilterTable(source_name);
                    sysFilterTable.setJobId(job_id);
                    sysFilterTable.setFilterField(sysFieldrule.getFieldName());
                    sysFilterTableRepository.save(sysFilterTable);
                    if (sysJobrelaRelateds != null && sysJobrelaRelateds.size() > 0) {
                        SysFilterTable sysFilterTable2 = null;
                        for (SysJobrelaRelated sysJobrelaRelated : sysJobrelaRelateds) {
                            //判断是第一次添加还是修改
//                        List<SysFilterTable> sysFilterTable1= sysFilterTableRepository.findByJobIdAndFilterTable(sysJobrelaRelated.getSlaveJobId(),source_name);
//                        if(sysFilterTable1!=null&&sysFilterTable1.size()>0){
//                            continue;
//                        }else {
                            sysFilterTable2 = new SysFilterTable();
                            sysFilterTable2.setFilterTable(source_name);
                            sysFilterTable2.setJobId(sysJobrelaRelated.getSlaveJobId());
                            sysFilterTable2.setFilterField(sysFieldrule.getFieldName());
                            sysFilterTableRepository.save(sysFilterTable2);
//                        }
                        }
                    }
                }

                //修改任务的状态为未激活
                long job_id1 = job_id;
                SysJobrela sysJobrelaById = sysJobrelaRespository.findById(job_id1);
                sysJobrelaById.setJobStatus("0");
                //修改子任务的状态未激活
                List<SysJobrelaRelated> lists = sysJobrelaRelatedRespository.findByMasterJobId(job_id);
                if (lists != null && lists.size() > 0) {
                    for (SysJobrelaRelated sysJobrelaRelated : lists) {
                        Optional<SysJobrela> sysJobrelaByIds = sysJobrelaRespository.findById(sysJobrelaRelated.getSlaveJobId());
                        sysJobrelaByIds.get().setJobStatus("0");
                    }
                }

                map.put("status", 1);
                map.put("message", "保存成功");
                map.put("data", sysFieldrules);
            } else {
                map.put("status", 1);
                map.put("message", "保存成功");
            }
        }catch (Exception e){
            StackTraceElement stackTraceElement = e.getStackTrace()[0];
            logger.error("*添加任务配置异常"+stackTraceElement.getLineNumber()+e);

            map.put("status", 0);
            map.put("message", "发生异常");
        }
        return map;
    }

    @Transactional
    @Override
    public Object deleteFieldrule(String source_name) {
        Map<Object, Object> map = new HashMap();
        if (repository.deleteBySourceName(source_name)) {
            map.put("status", 1);
            map.put("message", "删除成功");
        } else {
            map.put("status", 0);
            map.put("message", "没有找到删除目标");
        }
        return map;
    }

    @Override
    public Object linkTableDetails(SysDbinfo sysDbinfo, String tablename, Long job_id) {
        HashMap<Object, Object> map = new HashMap<>();
        Long type = sysDbinfo.getType();
        String sql = "";
        ArrayList<Object> data = new ArrayList<>();
        List<SysFieldrule> list = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        SysFieldrule sysFieldrule = null;
        if (type == 2) {
            sql = "select Column_Name as ColumnName,data_type as TypeName, (case when data_type = 'float' or data_type = 'int' or data_type = 'datetime' or data_type = 'bigint' or data_type = 'double' or data_type = 'decimal' then NUMERIC_PRECISION else CHARACTER_MAXIMUM_LENGTH end ) as length, NUMERIC_SCALE as Scale,(case when IS_NULLABLE = 'YES' then 0 else 1 end) as CanNull  from information_schema.columns where table_schema ='" + sysDbinfo.getDbname() + "' and table_name='" + tablename + "'";
            try {
                conn = DBConns.getMySQLConn(sysDbinfo);
                stmt = conn.prepareStatement(sql);
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    sysFieldrule = new SysFieldrule();
                    sysFieldrule.setFieldName(rs.getString("ColumnName"));
                    sysFieldrule.setType(rs.getString("TypeName"));
                    if (rs.getString("length") != null && !"".equals(rs.getString("length"))) {
                        sysFieldrule.setScale(rs.getString("length"));
                    } else {
                        sysFieldrule.setScale("0");
                    }
                    sysFieldrule.setNotNull(Long.valueOf(rs.getString("CanNull")));
                    if (rs.getString("Scale") != null && !"".equals(rs.getString("Scale"))) {
                        sysFieldrule.setAccuracy(rs.getString("Scale"));
                    } else {
                        sysFieldrule.setAccuracy("0");
                    }
                    System.out.println(sysFieldrule + "aaaaaaaaaaassssssss");

                    list.add(sysFieldrule);
                }
            } catch (SQLException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                StackTraceElement stackTraceElement = e.getStackTrace()[0];
                logger.error("*"+stackTraceElement.getLineNumber()+e);
                e.printStackTrace();
                return "连接异常@！";
            }
        } else if (type == 1) {
            sql = "SELECT COLUMN_NAME, DATA_TYPE, NVL(DATA_LENGTH,0), NVL(DATA_PRECISION,0), NVL(DATA_SCALE,0), NULLABLE, COLUMN_ID ,DATA_TYPE_OWNER FROM DBA_TAB_COLUMNS WHERE TABLE_NAME='" + tablename + "' AND OWNER='" + sysDbinfo.getSchema() + "'";
            System.out.println(sql);
            try {
                conn = DBConns.getOracleConn(sysDbinfo);
                stmt = conn.prepareStatement(sql);
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    sysFieldrule = new SysFieldrule();
                    sysFieldrule.setFieldName(rs.getString("COLUMN_NAME"));
                    sysFieldrule.setType(rs.getString("DATA_TYPE"));
                    if (rs.getString("NVL(DATA_LENGTH,0)") != null && !"".equals(rs.getString("NVL(DATA_LENGTH,0)"))) {
                        sysFieldrule.setScale(rs.getString("NVL(DATA_LENGTH,0)"));
                    } else {
                        sysFieldrule.setScale("0");
                    }
                    if (rs.getString("NULLABLE").equals("Y")) {
                        sysFieldrule.setNotNull(Long.valueOf(0));
                    } else {
                        sysFieldrule.setNotNull(Long.valueOf(1));
                    }
                    if (rs.getString("NVL(DATA_SCALE,0)")!= null && !"".equals(rs.getString("NVL(DATA_SCALE,0)"))) {
                        sysFieldrule.setAccuracy(rs.getString("NVL(DATA_SCALE,0)"));
                    } else {
                        sysFieldrule.setAccuracy("0");
                    }
                    System.out.println(sysFieldrule + "eeeeeeeeeeeeeeeee");
                    list.add(sysFieldrule);
                }
            } catch (SQLException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                StackTraceElement stackTraceElement = e.getStackTrace()[0];
                logger.error("*"+stackTraceElement.getLineNumber()+e);
                e.printStackTrace();
                return "连接异常@！";
            }
        }
        try {
            DBConns.close(stmt, conn, rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(list + "---d");
        System.out.println(data);
        // List<SysFieldrule> sysFieldruleList= sysFieldruleRepository.findByJobIdAndSourceName(job_id,tablename);
        map.put("status", "1");
        map.put("data", list);
        return map;
    }

    @Override
    public Object DestlinkTableDetails(SysDbinfo sysDbinfo, String tablename, Long job_id) {
        HashMap<Object, Object> map = new HashMap<>();
        HashMap<Object, Object> haspmap = new HashMap<>();
        ArrayList<SysFieldrule> data = new ArrayList<>();

        List<SysFieldrule> sysFieldruleList = sysFieldruleRepository.findByJobIdAndSourceName(job_id, tablename);
        List<SysFieldrule> list = sysFieldruleRepository.findByJobIdAndSourceNameAndVarFlag(job_id, tablename, Long.valueOf(2));
        List<SysDesensitization> sysDesensitizations = sysDesensitizationRepository.findByJobIdAndSourceTable(job_id, tablename);
        try {
            List<SysJobrela> sysJobrelaList = sysJobrelaRepository.findById(job_id.longValue());
            //查询目的数据库连接
            SysDbinfo  sysDbinfo2 = sysDbinfoRespository.findById(sysJobrelaList.get(0).getDestId().longValue());
            List<SysFiledType> sysFiledTypeList=null;
            //拿到源端的字段集合
            haspmap=(HashMap<Object, Object>)linkTableDetails(sysDbinfo,tablename,job_id);
            data= (ArrayList<SysFieldrule>) haspmap.get("data");
            //先把类型转换为目标端的类型
            for(int i=0;i<data.size();i++){
                sysFiledTypeList=new ArrayList<>();
                if(sysDbinfo.getType()!=sysDbinfo2.getType()) {
                    //去找到映射的字段类型
                    sysFiledTypeList = sysFiledTypeRepository.findBySourceTypeAndDestTypeAndSourceFiledType(String.valueOf(sysDbinfo.getType()), String.valueOf(sysDbinfo2.getType()), data.get(i).getType().toUpperCase());
                   if(sysFiledTypeList!=null&&sysFiledTypeList.size()>0) {
                       data.get(i).setType(sysFiledTypeList.get(0).getDestFiledType());
                   }
                }
            }
           if(list!=null&&list.size()>0) {
               //做过修改的替换进来，其余不变
               for (SysFieldrule sysFieldrule : list) {
                   for (int i = 0; i < data.size(); i++) {
                       if (sysFieldrule.getFieldName().equals(data.get(i).getFieldName())) {
                           data.set(i, sysFieldrule);
                       }
                   }
               }
           }
            map.put("status", "1");
            map.put("data", data);
            if (sysFieldruleList != null && sysFieldruleList.size() > 0) {
                map.put("destName", sysFieldruleList.get(0).getDestName());
            } else {
                List<SysTablerule> sysTablerules = sysTableruleRespository.findByJobIdAndSourceTableAndVarFlag(job_id, tablename, 2L);
                if (sysTablerules != null && sysTablerules.size() > 0) {
                    map.put("destName", sysTablerules.get(0).getDestTable());
                } else {
                    map.put("destName", null);
                }
            }
            if (sysDesensitizations != null && sysDesensitizations.size() > 0) {
                map.put("data2", sysDesensitizations);
            }
        } catch (Exception e) {
            StackTraceElement stackTraceElement = e.getStackTrace()[0];
            logger.error("*"+stackTraceElement.getLineNumber()+e);
            e.printStackTrace();
        }
        return map;
    }
}
