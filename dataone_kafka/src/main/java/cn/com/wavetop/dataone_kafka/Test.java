package cn.com.wavetop.dataone_kafka;

import cn.com.wavetop.dataone_kafka.entity.SysDbinfo;
import cn.com.wavetop.dataone_kafka.utils.HttpClientKafkaUtil;
import cn.com.wavetop.dataone_kafka.utils.JSONUtil;
import cn.com.wavetop.dataone_kafka.utils.SpringJDBCUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author yongz
 * @Date 2019/11/1、11:05
 */
public class Test {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {

        SysDbinfo source = SysDbinfo.builder()
                .host("192.168.1.140")
                .dbname("test")
                .user("test")
                .password("123456")
                .port(1521L)
                .type(1L)
                .schema("TEST")
                .build();
        JdbcTemplate register = SpringJDBCUtils.register(source);
        System.out.println(register);
//        register.execute("DECLARE V_SQL LONG;BEGIN V_SQL:='CREATE TABLE \"TEST\".\"employees\" (\"id\" NUMBER(19),\"fname\" VARCHAR2(30),\"lname\" VARCHAR2(30),\"birth\" VARCHAR2(64),\"hired\" date,\"separated\" date,\"job_code\" NUMBER(19),\"store_id\" NUMBER(19) )';EXECUTE IMMEDIATE V_SQL;EXCEPTION WHEN OTHERS THEN IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF; END; ");
//        Map<String, Object> name = new HashMap<>();
//        Map<String, String> config = new HashMap<>();
//        name.put("name", "file_source_test");
//        name.put("config", config);
//        config.put("connector.class", "FileStreamSource");
//        config.put("tasks.max", "1");
//        config.put("file", "/usr/local/soft/kafka_2.12-2.3.0/test/user.sql");
//        config.put("topic", "file_source_test");
//
//        String data = JSONUtil.toJSONString(name);
//
//        String delete = HttpClientKafkaUtil.deleteConnectors("192.168.1.187", 8083, "file_source_test");
//        System.out.println(delete);
//
//        String connector = HttpClientKafkaUtil.createConnector("192.168.1.187", 8083, data);
//        System.out.println(connector);
////        String s1 = HttpClientKafkaUtil.getConnectStatus("192.168.1.187", 8083, "test-a-file-source_21");
////        System.out.println(s1);
//        String  s = HttpClientKafkaUtil.getConnectorsDetails("192.168.1.187", 8083);
//        System.out.println(s);
//
//        s = HttpClientKafkaUtil.getConnectorsDetails("192.168.1.187", 8083);
//        System.out.println(s);

    }
}
