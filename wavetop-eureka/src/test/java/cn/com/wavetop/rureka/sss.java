package cn.com.wavetop.rureka;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class sss {
@Test
public void ss(){
    int index=1;

    String c;
    for(int i=0;i<1000;i++){
         index++;
        StringBuffer  stringBuffer=new StringBuffer("CREATE TABLE ");
        c="`index"+index+"`";
        stringBuffer.append(c);
        stringBuffer.append("(\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `job_id` int(11) NULL DEFAULT NULL,\n" +
                "  `delete_syncing_source` int(11) NULL DEFAULT NULL,\n" +
                "  `delete_sync` int(11) NULL DEFAULT NULL,\n" +
                "  `new_sync` int(11) NULL DEFAULT NULL,\n" +
                "  `newtable_source` int(11) NULL DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`) USING BTREE\n" +
                ") ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;\n" );

        System.out.println(stringBuffer);
    }
}
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
