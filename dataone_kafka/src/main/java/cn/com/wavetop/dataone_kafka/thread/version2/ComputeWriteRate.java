package cn.com.wavetop.dataone_kafka.thread.version2;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @Author yongz
 * @Date 2019/12/12、16:41
 */
public class ComputeWriteRate extends Thread {

    private long jobid;
    private String destTable;
    private JdbcTemplate jdbcTemplate;
    private Long fristCount;

    public ComputeWriteRate(long jobid, String destTable, JdbcTemplate jdbcTemplate) {
        this.jobid = jobid;
        this.jdbcTemplate = jdbcTemplate;
        this.destTable = destTable;
    }

    @Override
    public void run() {
        fristCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
        if (fristCount == null){
            fristCount = 0l;
        }
        boolean flag = true;
        while (flag) {
            Long starCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Long endCount = jdbcTemplate.queryForObject("select count(*) from " + destTable, Long.class);
            Double writeRate = (endCount-starCount)/2.0; // 写入速率

            Long writeAmount = endCount-fristCount; // 写入量

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
