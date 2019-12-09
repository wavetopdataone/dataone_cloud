package cn.com.wavetop.dataone_kafka.utils;

import cn.com.wavetop.dataone_kafka.entity.SysDbinfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.DriverManager;


/**
 * @Author yongz
 * @Date 2019/10/10、11:45
 *
 * 构建jdbctemplate工具类
 */
public class SpringJDBCUtils {


    //    @Bean
    public static JdbcTemplate register(SysDbinfo sysDbinfo)  {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        if (sysDbinfo.getType() == 2) {
            //1. 创建JdbcTemplate
            String url = "jdbc:mysql://" + sysDbinfo.getHost() + ":" + sysDbinfo.getPort() + "/" + sysDbinfo.getDbname() + "?characterEncoding=utf8&useUnicode=true&useSSL=false&serverTimezone=Asia/Shanghai";
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl(url);
            dataSource.setUsername(sysDbinfo.getUser());
            dataSource.setPassword(sysDbinfo.getPassword());
        } else if (sysDbinfo.getType() == 1) {
            //1. 创建JdbcTemplate
            String url = "jdbc:oracle:thin:@" + sysDbinfo.getHost() + ":" + sysDbinfo.getPort() + ":" + sysDbinfo.getDbname();
//            Class.forName("");
            DriverManager.setLoginTimeout(3);
            dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
            dataSource.setUrl(url);
            dataSource.setUsername(sysDbinfo.getUser());
            dataSource.setPassword(sysDbinfo.getPassword());
        }

        return new JdbcTemplate(dataSource);

    }


}
