package cn.com.wavetop.dataone_kafka.thread.version2;

import cn.com.wavetop.dataone_kafka.config.SpringContextUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestTemplate;

/**
 * @Author yongz
 * @Date 2019/12/12、16:41
 */
public class ComputeWriteRate extends Thread {

    // 注入restTemplate
    private RestTemplate restTemplate = (RestTemplate) SpringContextUtil.getBean("restTemplate");

    private long jobid;
    private String destTable;
    private JdbcTemplate jdbcTemplate;
    private Long fristCount;
    private int sync_range;

    public ComputeWriteRate(long jobid, String destTable, JdbcTemplate jdbcTemplate) {
        this.jobid = jobid;
        this.jdbcTemplate = jdbcTemplate;
        this.destTable = destTable;
    }

    @Override
    public void run() {

        sync_range = 1;
        int index = 0;
        fristCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
        if (fristCount == null){
            fristCount = 0l;
        }
        Long lastEndCount = fristCount;

        boolean flag = true;
        while (flag) {
            Long starCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Long endCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
            Double writeRate = (endCount-starCount)/1.0; // 写入速率

            if (writeRate<1000){
                writeRate = 1000.0;     // 待优化
            }
            Long writeAmount = endCount-fristCount; // 写入量
            Long realWriteAmount = endCount-lastEndCount;//往实时表中更新
            lastEndCount = endCount;
            if (writeAmount != 0) {
                restTemplate.getForObject("http://DATAONE-WEB/toback/updateWriteRate/" + jobid + "?destTable=" + destTable + "&realWriteAmount=" + realWriteAmount + "&writeAmount=" + writeAmount + "&writeRate=" + writeRate, Object.class);
            }
//            System.out.println(destTable+"--------"+writeRate+"--------------"+writeAmount+"---------------"+realWriteAmount+"------------"+fristCount);



            if (sync_range == 1){
                if (realWriteAmount == 0){
                    index++;
                }
                if (index == 1000){
                    flag = false;
                }
            }

            try {
                Thread.sleep(8000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
