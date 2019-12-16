package cn.com.wavetop.dataone_kafka.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

@FeignClient(value = "DATAONE-WEB")
@Component
public interface ToBackClient {


    /**
     * 根据jobid重置监听表
     * @param jobId
     * @return
     */
    @PutMapping("/toback/resetMonitoring/{jobId}")
    public void resetMonitoring( @PathVariable Long jobId) ;

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
    @GetMapping("/toback/find_range/{Id}")
    public Object findRangeByJobId(@PathVariable long Id) ;
    /**
     * 更新读监听数据
     * @param Id
     * @param readData
     */
    @GetMapping("/toback/readmonitoring/{Id}")
    public void updateReadMonitoring(@PathVariable long Id, @RequestParam final Long readData,@RequestParam final String table);


    /**
     * 更新写监听数据
     * @param Id
     * @param writeData
     */
    @GetMapping("/toback/writemonitoring/{Id}")
    public void updateWriteMonitoring(@PathVariable long Id,@RequestParam Long writeData,String table);

    /**
     * 更新写监听数据
     * @param Id
     * @param
     */
    @GetMapping("/toback/find_destTable/{Id}")
    public Object monitoringTable(@PathVariable long Id);

    /**
     * 更新写监听数据
     * @para
     * @param
     */
    @GetMapping("/toback/updateReadRate/{readRate}")
    public void monitoringTable(@PathVariable Long readRate,@RequestParam final Long jobId);

    /**
     * 更新写监听数据
     * @para
     * @param
     * @param jobId
     * @param disposeRate
     * @param errorData
     */
    @GetMapping("/toback/updateDisposeRateAndError/{jobId}")
    public void updateDisposeRateAndError(@PathVariable int jobId, @RequestParam final double disposeRate, @RequestParam final int errorData);

    /**
     * 错误队列
     * @para
     * @param
     * @param jobId
     * @param optContext
     * @param content
     */
    @GetMapping("/toback/InsertLogError/{jobId}")
    public void InsertLogError(@PathVariable int jobId, @RequestParam final Object optContext, @RequestParam final Object content);

    /**
     * kafka需要的同步数据
     */
    @GetMapping("/toback/kafkaFiled/{jobId}")
    public Object kafkaFiled(@PathVariable Long jobId);
}
