package com.cn.wavetop.dataone.config.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;


@ControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * 处理自定义的业务异常
     * @param req
     * @param e
     * @return
     */
    @ExceptionHandler(value = BizException.class)
    @ResponseBody
    public ResultBody bizExceptionHandler(HttpServletRequest req, BizException e){
        logger.error("发生业务异常！原因是：{}",e.getErrorMsg());
        return ResultBody.error(e.getErrorCode(),e.getErrorMsg());
    }


    /**
     * 处理空指针的异常
     * @param req
     * @param e
     * @return
     */
    @ExceptionHandler(value =NullPointerException.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, NullPointerException e){
        logger.error("*发生空指针异常！原因是:",e);
        return ResultBody.error(CommonEnum.BODY_NOT_MATCH);
    }

    /**
     * 处理请求方法不支持的异常
     * @param req
     * @param e
     * @return
     */
    @ExceptionHandler(value = HttpRequestMethodNotSupportedException.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, HttpRequestMethodNotSupportedException e){
        logger.error("发生请求方法不支持异常！原因是:",e);
        return ResultBody.error(CommonEnum.REQUEST_METHOD_SUPPORT_ERROR);
    }
    //类型转换异常
    @ExceptionHandler(ClassCastException.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, ClassCastException ex) {
        logger.error("类型转换异常",ex);
        return ResultBody.error(CommonEnum.INTERNAL_SERVER_ERROR);
    }

    //IO异常
    @ExceptionHandler(IOException.class)
    @ResponseBody
    public ResultBody iOExceptionHandler(IOException ex) {
        logger.error("IO异常",ex);
        return ResultBody.error(CommonEnum.INTERNAL_SERVER_ERROR);
    }
    //数据下标越界
    @ExceptionHandler(ArrayIndexOutOfBoundsException.class)
    @ResponseBody
    public ResultBody ArrayIndexOutOfBoundsExceptionHandler(ArrayIndexOutOfBoundsException ex) {
        logger.error("数据下标越界",ex);
        return ResultBody.error(CommonEnum.INTERNAL_SERVER_ERROR);
    }

    /**
     * 处理其他异常
     * @param req
     * @param e
     * @return
     */
    @ExceptionHandler(value =Exception.class)
    @ResponseBody
    public ResultBody exceptionHandler(HttpServletRequest req, Exception e){
        logger.error("未知异常！原因是:",e);
        return ResultBody.error(CommonEnum.INTERNAL_SERVER_ERROR);
    }
}
