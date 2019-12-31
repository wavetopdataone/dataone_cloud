package cn.com.wavetop.dataone_kafka.consumer;


import cn.com.wavetop.dataone_kafka.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CustomConsumer3 extends Thread {

    private long startTime; //记录程序开始时间
    private long offset = 0; // 记录文件读取的行数

    @Override
    public void run() {
        {
            startTime = System.currentTimeMillis();
            boolean flag = true; // 线程终止标识
            StringBuffer result = new StringBuffer();
            File file = new File("offset" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()));

            try {
                InputStreamReader reader = new InputStreamReader(new FileInputStream(new File("dataoneinfo-2019-12-27.0.log")), "utf-8");
                BufferedReader br = new BufferedReader(reader);
                String payload = null;
                if (file.exists()) {
                    long last_offset = Long.parseLong(FileUtils.readTxtFile(file));
                    System.out.println(last_offset);
                    offset = last_offset;
                    for (long i= 0; i<last_offset;i++){
                        br.readLine();
                    }
                }
                while (flag) {
                    payload = br.readLine();
                    if (payload == null) {
                        Thread.sleep(1000);
                        continue;
                    }

                    {
                        // 你在这里面写你的逻辑
                        System.out.println(payload);
                    }

                    {
                        // 记录offset到本地

                        offset++;
                        FileUtils.writeTxtFile(String.valueOf(offset), file);
                    }
                    if (System.currentTimeMillis() - startTime >= 50000) {
                        // 超过24小时终止当前线程
                        flag = false;
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}