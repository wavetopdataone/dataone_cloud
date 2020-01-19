package cn.com.wavetop.rureka;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class sss {

    @Test
    public void show(){
        Set<String> set=new TreeSet<>();
        set.add("c");
        set.add("b");
        set.add("a");
        set.add("2");
        set.add("阿你");
        set.add("1");
        set.add("爱");
        set.add("A");

        char sasas = '爱';
        char sasas2 = '阿';

        int index1= sasas;
        int index2= sasas2;

        System.out.println(index1);
        System.out.println(index2);

        for(String a:set){
            System.out.println(a);
        }
    }
}
