package com.cn.wavetop.dataone.service.impl;

import com.cn.wavetop.dataone.dao.SysDeptRepository;
import com.cn.wavetop.dataone.dao.SysUserJobrelaRepository;
import com.cn.wavetop.dataone.dao.SysUserRepository;
import com.cn.wavetop.dataone.dao.SysUserRoleRepository;
import com.cn.wavetop.dataone.entity.SysDept;
import com.cn.wavetop.dataone.entity.SysJobrela;
import com.cn.wavetop.dataone.entity.SysUser;
import com.cn.wavetop.dataone.entity.SysUserRole;
import com.cn.wavetop.dataone.entity.vo.SysUserDeptVo;
import com.cn.wavetop.dataone.entity.vo.ToData;
import com.cn.wavetop.dataone.entity.vo.ToDataMessage;
import com.cn.wavetop.dataone.service.SysDeptService;
import com.cn.wavetop.dataone.util.LogUtil;
import com.cn.wavetop.dataone.util.PermissionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.*;
import java.util.function.Consumer;

@Service
public class SysDeptServiceImpl implements SysDeptService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SysDeptRepository sysDeptRepository;
    @Autowired
    private SysUserRepository sysUserRepository;
    @Autowired
    private SysUserJobrelaRepository sysUserJobrelaRepository;
    @Autowired
    private SysUserRoleRepository sysUserRoleRepository;
    @Autowired
    private LogUtil logUtil;
    //超级管理员查询部门和部门里面的管理员或者管理员查询自己部门和自己部门里的员工数量
    @Override
    public Object selDept() {
        Map<Object,Object> map=new HashMap<>();
        //Pageable pageable = new PageRequest(current - 1, size, Sort.Direction.DESC, "id");
        //Page<SysDept> list = sysDeptRepository.findAll(pageable);
        List<SysDept> list=sysDeptRepository.findAll();
        SysUserDeptVo sysUserDeptVo;
       List<SysUserDeptVo> userDeptVoList=new ArrayList<>();
       int index=0;
       if(PermissionUtils.isPermitted("1")) {
          // for (SysDept sysDept : list.getTotalElements()) {
           for (SysDept sysDept : list) {
               sysUserDeptVo = new SysUserDeptVo(sysDept.getId(), sysDept.getDeptName(), sysUserRepository.countByDeptIdandPerms(sysDept.getId()));
               if(sysUserDeptVo.getCountUser()==0){
                   userDeptVoList.add(index,sysUserDeptVo);
                     index++;
               }else{
                   userDeptVoList.add(index,sysUserDeptVo);
               }
           }
           map.put("status","1");
           map.put("data",userDeptVoList);
           map.put("size",userDeptVoList.size());
       }else if(PermissionUtils.isPermitted("2")){
         Optional<SysUser> sysUser= sysUserRepository.findById(PermissionUtils.getSysUser().getId());
         Optional<SysDept> sysDept=sysDeptRepository.findById(sysUser.get().getDeptId());
         if(sysDept!=null) {
             List<SysUser> lists = sysUserRepository.findByDeptId(sysUser.get().getDeptId());
             sysUserDeptVo = new SysUserDeptVo(sysDept.get().getId(), sysDept.get().getDeptName(), Long.valueOf(lists.size()));
             userDeptVoList.add(sysUserDeptVo);
             map.put("status", "1");
             map.put("data", userDeptVoList);
             map.put("size", userDeptVoList.size());
         }else{
             return ToData.builder().status("2").message("该用户没有分组").build();

         }
       }else{
           return ToData.builder().status("0").message("权限不足").build();
       }
       return map;
    }

    @Override
    public Object addDept(SysDept sysDept) {
        List<SysDept> sysUserList = sysDeptRepository.findByDeptName(sysDept.getDeptName());
        if (sysUserList != null && sysUserList.size() > 0) {
            return ToDataMessage.builder().status("2").message("该分组已经存在").build();
        } else {
            if (PermissionUtils.isPermitted("1")) {
                sysDept.setCreateTime(new Date());
                sysDept.setCreateUser(PermissionUtils.getSysUser().getLoginName());
                SysDept sysDept1 = sysDeptRepository.save(sysDept);
                if(sysDept1!=null){
                    logUtil.saveUserlogDept(sysDept, null,null, "com.cn.wavetop.dataone.service.impl.SysDeptServiceImpl.addDept", "添加小组");
                }
                return ToDataMessage.builder().status("1").message("添加成功").build();
            } else {
                return ToDataMessage.builder().status("0").message("权限不足").build();
            }
        }
    }

    @Override
    public Object updatDept(SysDept sysDept) {
        if(PermissionUtils.isPermitted("1")){
            Optional<SysDept> list=sysDeptRepository.findById(sysDept.getId());
            SysDept sysDeptOld=list.get();
            list.get().setDeptName(sysDept.getDeptName());
            list.get().setUpdateTime(new Date());
            list.get().setUpdateUser(PermissionUtils.getSysUser().getLoginName());
            List<SysDept> sysDeptList=sysDeptRepository.findByDeptName(sysDept.getDeptName());
            if(sysDeptList!=null&&sysDeptList.size()>0){
                return ToDataMessage.builder().status("2").message("该分组已存在").build();
            }else {
                SysDept sysDept1 = sysDeptRepository.save(sysDept);
                if(sysDept1!=null){
                    logUtil.saveUserlogDept(sysDept, sysDeptOld,null, "com.cn.wavetop.dataone.service.impl.SysDeptServiceImpl.updatDept", "修改小组");
                }
                return ToDataMessage.builder().status("1").message("修改成功").build();
            }
        }else{
            return ToDataMessage.builder().status("0").message("权限不足").build();
        }
    }

    @Transactional
    @Override
    public Object delDept(String deptName) {
        if(PermissionUtils.isPermitted("1")){
            List<SysDept> list=sysDeptRepository.findByDeptName(deptName);
            List<SysUser> sysUserList=sysUserRepository.findByDeptId(list.get(0).getId());
            boolean flag=false;
            for(SysUser sysUser:sysUserList){
              flag=sysUserJobrelaRepository.existsAllByUserId(sysUser.getId());
                System.out.println(flag);
                if(flag) {
                    return ToDataMessage.builder().status("2").message("该分组下还有任务").build();
                }
                }
           if(sysUserList!=null&&sysUserList.size()>0){
               return ToDataMessage.builder().status("2").message("该部门下面还有人员").build();
           }else{
               sysUserRepository.deleteById(list.get(0).getId());
               sysUserRoleRepository.deleteByUserId(list.get(0).getId());
               sysDeptRepository.deleteByDeptName(deptName);
               logUtil.saveUserlogDept(list.get(0), null,sysUserList, "com.cn.wavetop.dataone.service.impl.SysDeptServiceImpl.delDept", "删除小组");
               return ToDataMessage.builder().status("1").message("删除成功").build();
           }
        }else{
            return ToDataMessage.builder().status("0").message("权限不足").build();
        }

    }


}
