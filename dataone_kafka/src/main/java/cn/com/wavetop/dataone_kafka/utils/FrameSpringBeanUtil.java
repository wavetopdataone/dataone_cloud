package cn.com.wavetop.dataone_kafka.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class FrameSpringBeanUtil implements ApplicationContextAware {
    private static ApplicationContext applicationContext;
 
    @Override
    public void setApplicationContext(ApplicationContext arg0) throws BeansException {
        applicationContext = arg0;
    }
 
    @SuppressWarnings("unchecked")
    public static <T> T getBean(String name) {
        return (T) applicationContext.getBean(name);
    }
 
    public static <T> T getBean(Class<T> cls) {
        return applicationContext.getBean(cls);
    }
}
