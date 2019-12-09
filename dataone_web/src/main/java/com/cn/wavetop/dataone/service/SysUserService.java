package com.cn.wavetop.dataone.service;

import com.cn.wavetop.dataone.entity.SysUser;

public interface SysUserService  {

    Object login(String name,String password);
    Object loginOut();
    Object findAll();
    Object findById();
    Object update(SysUser sysUser);
    Object addSysUser(SysUser sysUser,String id);
    Object delete(String userName);
    Object findRolePerms(String userName);
    Object findByUserName(String userName);
    Object findUserByDept(Long deptId);
    Object updateUser(Long id,Long DeptId);
    Object updateStatus(Long id,String status);
    Object selSysUser(Long userId);
    Object HandedTeam(Long id,Long userId);
    Object seleUserBystatus(String status);
    Object selectUserByParentId(Long userId);

    Object sendEmail(String email);
    Object editPasswordByEmail(String email,String password);
    Object codeEquals(String authCode);
    //绑定邮箱
    Object bindEmail(String email,String emailPassword);
//个人设置的个人详情
    Object Personal();
        //查询分组和分组下面的人
    Object findDeptAndUser();

    Object updPassword(Long userId,String password,String newPassword);
    //修改超级管理员邮箱
    Object updSuperEmail(Long userId,String password,String newEmail,String emailPassword);
    //修改用户邮箱
    Object updUserEmail(Long userId, String password, String newEmail);
}
