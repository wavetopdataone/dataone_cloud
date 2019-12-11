package cn.com.wavetop.dataone_kafka.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "DATAONE-WEB")
//@RequestMapping("/toback")
@Component
public interface ToBackClient {




    /**
     * 根据jobid查询数据信息
     * @param jobId
     * @return
     */
    @GetMapping("/toback/findById/{jobId}")
    public Object findDbinfoById( @PathVariable Long jobId) ;

    /**
     * 查询同步数据类型
     * @param Id
     * @return
     */
    @GetMapping("/find_range/{Id}")
    public Object findRangeByJobId(@PathVariable long Id) ;
    /**
     * 更新读监听数据
     * @param Id
     * @param readData
     */
    @GetMapping("/readmonitoring/{Id}")
    public void updateReadMonitoring(@PathVariable long Id, @RequestParam final Long readData,@RequestParam final String table);


    /**
     * 更新写监听数据
     * @param Id
     * @param writeData
     */
    @GetMapping("/writemonitoring/{Id}")
    public void updateWriteMonitoring(@PathVariable long Id,@RequestParam Long writeData,String table);

    /**
     * 更新写监听数据
     * @param Id
     * @param
     */
    @GetMapping("/find_destTable/{Id}")
    public Object monitoringTable(@PathVariable long Id);

    /**
     * 更新写监听数据
     * @para
     * @param
     */
    @GetMapping("/updateReadRate/{readRate}")
    public void monitoringTable(@PathVariable Long readRate,@RequestParam final Long jobId);

    /**
     * 更新写监听数据
     * @para
     * @param
     * @param jobId
     * @param disposeRate
     * @param errorData
     */
    @GetMapping("/updateDisposeRateAndError/{jobId}")
    public void updateDisposeRateAndError(@PathVariable int jobId, @RequestParam final double disposeRate, @RequestParam final int errorData);

    /**
     * 错误队列
     * @para
     * @param
     * @param jobId
     * @param optContext
     * @param content
     */
    @GetMapping("/InsertLogError/{jobId}")
    public void InsertLogError(@PathVariable int jobId, @RequestParam final Object optContext, @RequestParam final Object content);

    /**
     * kafka需要的同步数据
     */
    @GetMapping("/kafkaFiled/{jobId}")
    public Object kafkaFiled(@PathVariable Long jobId);
}
