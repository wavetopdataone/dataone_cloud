package com.cn.wavetop.dataone;

import com.cn.wavetop.dataone.dao.SysUserRepository;
import com.cn.wavetop.dataone.entity.SysFieldrule;
import com.cn.wavetop.dataone.entity.SysJobrela;
import com.cn.wavetop.dataone.entity.SysUser;
import com.cn.wavetop.dataone.entity.SysUserJobrela;
import com.cn.wavetop.dataone.util.DBConns;
import com.cn.wavetop.dataone.util.DateUtil;
import org.apache.shiro.crypto.SecureRandomNumberGenerator;
import org.apache.shiro.crypto.hash.Md5Hash;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

public class test {


    @Test
    public void asq(){
//        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
//
//        String nowDate = dfs.format(new Date());//几号
////        String hour = nowDate.substring(0, 2);
////        String minue = nowDate.substring(3, 5);
//        String yesterDay = DateUtil.dateAdd(nowDate, -1);//昨天
//        System.out.println(nowDate);
//        System.out.println(yesterDay);
//       Date a=new Date("1575529091543");
        List<SysJobrela> list=new ArrayList<>();
        SysJobrela sysUserJobrela=new SysJobrela();
        sysUserJobrela.setId(Long.valueOf(95));
        sysUserJobrela.setJobName("test1");
        SysJobrela sysUserJobrela1=new SysJobrela();
        sysUserJobrela1.setId(Long.valueOf(96));
        sysUserJobrela1.setJobName("test2");
        SysJobrela sysUserJobrela4=new SysJobrela();
        sysUserJobrela4.setId(Long.valueOf(97));
        sysUserJobrela4.setJobName("test2");
        list.add(sysUserJobrela);
        list.add(sysUserJobrela1);
        list.add(sysUserJobrela4);
        List<SysJobrela> list1=new ArrayList<>();
        SysJobrela sysUserJobrela2=new SysJobrela();
        sysUserJobrela2.setId(Long.valueOf(96));
        sysUserJobrela2.setJobName("test3");
        list1.add(sysUserJobrela2);
        System.out.println(list);
        System.out.println(list1);
       for(SysJobrela sysJobrela:list1){
           for(int i =0;i<list.size();i++ ){
               if(sysJobrela.getId()==list.get(i).getId()){
                   list.get(i).setJobName("dsadasdas");
               }
           }
       }
        System.out.println(list);
        System.out.println(list1);
    }
    @Test
public void s(){

    List<SysJobrela> list=new ArrayList<>();
        SysJobrela sysUserJobrela=new SysJobrela();
    sysUserJobrela.setId(Long.valueOf(95));
    sysUserJobrela.setJobName("test1");
        SysJobrela sysUserJobrela1=new SysJobrela();
        sysUserJobrela1.setId(Long.valueOf(96));
        sysUserJobrela1.setJobName("test2");
    list.add(sysUserJobrela);
    list.add(sysUserJobrela1);
    List<SysJobrela> list1=new ArrayList<>();
    SysJobrela sysUserJobrela2=new SysJobrela();
        sysUserJobrela2.setId(Long.valueOf(95));
        sysUserJobrela2.setJobName("test1");
    list1.add(sysUserJobrela2);
        for (SysJobrela str:list) {
            if(list.contains(str)){
                list.remove(str);
            }
        }
        for (SysJobrela str:list1) {
            str.setJobStatus("1");
            list.add(str);
        }
//    for(SysJobrela set:list1){
//        list.add(set);
//    }
//        Set set = new HashSet();
//        List<SysJobrela> listNew=new ArrayList<>();
//        set.addAll(list);
//        listNew.addAll(set);
    for (SysJobrela s:list){
        System.out.println(s+"----");
    }
}
    /**
     * 日期转星期
     *
     * @param datetime
     * @return
     */
    public static String dateToWeek(String datetime) {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        String[] weekDays = { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };
        Calendar cal = Calendar.getInstance(); // 获得一个日历
        Date datet = null;
        try {
            datet = f.parse(datetime);
            cal.setTime(datet);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1; // 指示一个星期中的某天。
        if (w < 0)
            w = 0;
        return weekDays[w];
    }

     @Test
    public void sqa(){
//         SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
//
//         String nowDate = dfs.format(new Date());//几号
         Date date=new Date();
         SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");// 设置日期格式
         String ab=dfs.format(date);
//         if(num.equals("7")) {
             DateUtil.dateAdd(ab, -6);
//         }
         System.out.println( DateUtil.dateAdd(ab, -6));
//         System.out.println(d-a);
     }

     @Autowired
     private RedisTemplate<String,Object> redisTemplate;
     @Test
    public void sawq(){
         List<String> list=new ArrayList<>();
         list.add("1");
      redisTemplate.opsForValue().set("a",list);
         System.out.println(redisTemplate.opsForValue().get("a"));
     }
     @Test
    public void encryptPassword() {
        String salt = new SecureRandomNumberGenerator().nextBytes().toHex(); //生成盐值
        String ciphertext = new Md5Hash("Aa111111", salt, 3).toString(); //生成的密文
        String[] strings = new String[]{salt, ciphertext};
         System.out.println(salt);
         System.out.println(ciphertext);
    }

    //驗證密碼
//        String ciphertext = new Md5Hash("88888","0ae975a6aeb859797f60e98c575ee12c",3).toString(); //生成的密文
//       String password="a3be8ba9c44a062345e2a210661df3b8";
//        System.out.println(ciphertext);
//                if(ciphertext.equals(password)){
//                    System.out.println(true);
//                }
//        String splits = "id,id,NUMBER,22,$,name,name,VARCHAR2,255,$";
//        String[]  splitss= splits.replace("$","@").split(",@,");
//     for(String s:splitss){
//         System.out.println(s);
//     }
//        List<SysFieldrule> list = new ArrayList<>();
////        String[] a="aa,bb,cc,".split(",");
////        for(int b=0;b<a.length;b++){
////            System.out.println(a[b]);
////        }
//        SysFieldrule s=new SysFieldrule();
//         s.setSourceName("123");
//         s.setDestName("abc");
//        SysFieldrule s1=new SysFieldrule();
//        s1.setSourceName("456");
//        s1.setDestName("abc");
//        SysFieldrule s3=new SysFieldrule();
//        s3.setSourceName("789");
//        s3.setDestName("abc");
//        SysFieldrule s2=new SysFieldrule();
//        s2.setSourceName("123");
//        s2.setDestName("abc");
//        SysFieldrule s4=new SysFieldrule();
//        s4.setSourceName("456");
//        s4.setDestName("abc");
//        list.add(s);
//        list.add(s3);
//    list.add(s1);
//    Set<SysFieldrule> sysFieldrules=new HashSet<SysFieldrule>();
//        sysFieldrules.add(s2);
//        sysFieldrules.add(s4);
//        Iterator<SysFieldrule> iterator = list.iterator();
//        while (iterator.hasNext()) {
//            SysFieldrule num = iterator.next();
//            if (sysFieldrules.contains(num)) {
//                iterator.remove();
//            }
//        }
//        for (SysFieldrule b : list) {
//            System.out.println(b);
//        }
        //        Long a = Long.valueOf(5);
//        System.out.println(a.longValue() + a);
//        List<String> list = new ArrayList<>();
//        list.add("aa");
//        list.add("aa");
//        list.add("aa");
//        list.add("bb");
//        List<String> newList = new ArrayList();
//        newList.add("aa");
//        Iterator<String> iterator = list.iterator();
//        while (iterator.hasNext()) {
//            String num = iterator.next();
//            if (newList.contains(num)) {
//                iterator.remove();
//            }
//        }
//        for (String b : list) {
//            System.out.println(b);
//        }

}
