package cn.com.wavetop.dataone_kafka.connect;

import cn.com.wavetop.dataone_kafka.connect.model.Schema;
import cn.com.wavetop.dataone_kafka.utils.JSONUtil;
import cn.com.wavetop.dataone_kafka.utils.PrassingUtil;
import net.sf.jsqlparser.JSQLParserException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author yongz
 * @Date 2019/12/4、14:31
 */
public class TestModel {

    public static void main(String[] args) throws JSQLParserException {
        Schema schema = mysqlToSchema("DECLARE V_SQL LONG;BEGIN V_SQL:='CREATE TABLE \"TEST\".\"employees\" (\"id\" NUMBER(19),\"fname\" VARCHAR2(30),\"lname\" VARCHAR2(30),\"birth\" VARCHAR2(64),\"hired\" date,\"separated\" date,\"job_code\" NUMBER(19),\"store_id\" NUMBER(19) )';EXECUTE IMMEDIATE V_SQL;EXCEPTION WHEN OTHERS THEN IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF; END; ", 1);
//        System.out.println(schema);
//        getStringTime(1575455606000L);
        System.out.println(schema);
        HashMap<String, Schema> schemas = new HashMap<>();
        schemas.put(schema.getName(), schema);
        toJsonString2("INSERT INTO \"TEST\".\"employees\"(\"id\",\"fname\",\"lname\",\"birth\",\"hired\",\"separated\",\"job_code\",\"store_id\") VALUES ('59641','chen59641','haixiang59641','2019-10-09 23:17:40',TO_DATE('2019-10-09 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE('2019-10-09 00:00:00','YYYY-MM-DD HH24:MI:SS'),'1','59641')", schemas, 1);

    }

    /**
     * @param time timestamp
     * @desc 时间戳转字符串
     * @example timestamp=1558322327000
     **/
    public static String getStringTime(Long time, String type) {
        return new SimpleDateFormat(type).format(new Date(time));
    }

    /**
     * @param time timestamp
     * @return
     * @desc 字符串转时间戳
     * @example timestamp=1558322327000
     */
    public static long getTimestamp(String time, String type) {
        try {
            return new SimpleDateFormat(type).parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("报错了！");
        }
        return 0;
    }

    /**
     * 解析mysql的create语句
     *
     * @param creat_table
     * @return
     */
    public static Schema mysqlToSchema(String creat_table, int dbType) {
        ArrayList fileds = new ArrayList<>();

        String tableName = "";
        // todo 表名获取待优化
        String[] s1 = creat_table.split(" ");
        for (String s : s1) {
            if (s.contains(".")) {
                s = s.split("\\.")[1];
                if (s.contains("'") || s.contains("\"")) {
                    s = s.substring(1, s.length() - 1);
                }
                tableName = s;
            }
//            System.out.println(s);
        }
        String[] strings = creat_table.substring(creat_table.indexOf("(") + 1, creat_table.lastIndexOf(")")).split(",");
//        System.out.println(Arrays.toString(strings));

        String filedType;
        String field; // 字段
        HashMap map;
        for (int i = 0; i < strings.length; i++) {
            map = new HashMap<>();
            filedType = strings[i].split(" ")[1];
            if (filedType.equalsIgnoreCase("TINYINT")) {
                filedType = "int8";
            } else if (filedType.equalsIgnoreCase("SMALLINT")) {
                filedType = "int16";
            } else if (filedType.equalsIgnoreCase("INT")) {
                filedType = "int32";
            } else if (filedType.equalsIgnoreCase("BIGINT") || filedType.contains("NUMBER")) {
                filedType = "int64";
            } else if (filedType.equalsIgnoreCase("FLOAT")) {
                filedType = "float32";
            } else if (filedType.equalsIgnoreCase("DOUBLE")|| filedType.contains("DECIMAL")) {
                filedType = "float64";
            } else if (filedType.contains("VARCHAR")) {
                filedType = "string";
            } else if (filedType.contains("VARBINARY")) {
                filedType = "bytes";
            } else if (filedType.contains("DATE") || filedType.contains("date") || filedType.contains("TIME") || filedType.contains("time")) {
                filedType = "int64";
                map.put("name", "org.apache.kafka.connect.data.Timestamp");
                map.put("version", 1);
            }

            field = strings[i].split(" ")[0];
            if (field.contains("'") || field.contains("\"")) {
                field = field.substring(1, field.length() - 1);
            }
            map.put("type", filedType);
            map.put("optional", true);
            if (dbType == 1) {
                field = field.toUpperCase();
            }
            map.put("field", field);

            fileds.add(map);
            map = null;
        }


        Schema schema = Schema.builder().type("struct").fields(fileds).name(tableName).build();
//        HashMap<Object, Object> map = new HashMap<>();
//        map.put("ID", 1);
//        map.put("NAME", "帅啊");
//
//        HashMap<Object, Object> map2 = new HashMap<>();
//        map2.put("schema", hehe);
//        map2.put("payload", map);
//
//        System.out.println(map2);
//        String s = JSONUtil.toJSONString(map2);
//        System.out.println(s);
        return schema;
    }

    public static String toJsonString2(String insertSql, HashMap<String, Schema> schemas, int dbType) throws JSQLParserException {
        HashMap<Object, Object> map = new HashMap<>();
        List<String> insert_columns = null;
        List<String> insert_values = null;
        try {
            insert_columns = PrassingUtil.get_insert_column(insertSql);
            insert_values = PrassingUtil.get_insert_values(insertSql);

        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        String insert_table = PrassingUtil.get_insert_table(insertSql);

        if (insert_table.contains("'") || insert_table.contains("\"")) {
            insert_table = insert_table.substring(1, insert_table.length() - 1);
        }
        Schema schema = schemas.get(insert_table);
        String column;
        String value;
        for (int i = 0; i < insert_columns.size(); i++) {
            column = insert_columns.get(i);
            value = insert_values.get(i);
            if (column.contains("'") || column.contains("\"")) {
                column = column.substring(1, column.length() - 1);
            }
            if (dbType == 1) {
                column = column.toUpperCase();
            }
            if ((value.contains("DATE") || value.contains("data")) && value.contains("(")) {
                String[] split = value.substring(value.indexOf("("), value.lastIndexOf(")")).split(",");
                split[0] = split[0].substring(2, split[0].length() - 1); // 获取时间
                System.out.println(split[0]);
                if (split[0].length() <= 4) {
//                    System.out.println(getTimestamp(split[0], "yyyy"));
                    map.put(column, getTimestamp(split[0], "yyyy"));
                } else if (split[0].length() <= 10) {
//                    System.out.println(getTimestamp(split[0], "yyyy-MM-dd"));
                    map.put(column, getTimestamp(split[0], "yyyy-MM-dd"));

                } else if (split[0].length() <= 19) {
//                    System.out.println(getTimestamp(split[0], "yyyy-MM-dd HH:mm:ss"));
                    map.put(column, getTimestamp(split[0], "yyyy-MM-dd HH:mm:ss"));
                }

            } else {
                if (value.contains("'") || value.contains("\"")) {
                    value = value.substring(1, value.length() - 1);
                }


                List<Map> schemaFields = schema.getFields();
                for (Map schemaField : schemaFields) {
                    String field = (String) schemaField.get("field");
                    if (field.equalsIgnoreCase(column)) {

                        // 首先她两必须相等
                        System.out.println(field);
                        String type = (String) schemaField.get("type");
                        if (type.contains("int")) {
                            map.put(column, Long.parseLong(value));
                        } else if (type.contains("float")) {
                            map.put(column, Double.parseDouble(value));
                        } else if (type.contains("boolean")) {
                            map.put(column, Boolean.parseBoolean(value));
                        } else {
                            map.put(column, value);
                        }

                    }
                }

            }


        }

//        System.out.println(insertSql);
//        String[] insert = insertSql.split("VALUES");
//        String[] fields = insert[0].substring(insert[0].indexOf("(") + 1, insert[0].lastIndexOf(")")).split(",");
//        String[] values = insert[1].substring(insert[1].indexOf("(") + 1, insert[1].lastIndexOf(")")).split(",");
//        System.out.println(fields.length);
//        System.out.println(values.length);
//        for (int i = 0; i < fields.length; i++) {
////
//            if (values[i].contains("'") || values[i].contains("\"")) {
//                values[i] = values[i].substring(1, values[i].length() - 1);
//            }
//            if (fields[i].contains("'") || fields[i].contains("\"")) {
//                fields[i] = fields[i].substring(1, fields[i].length() - 1);
//            }
//            System.out.println(fields[i]+values[i]);
//            map.put(fields[i], values[i]);
////            List<Map> schemaFields = schema.getFields();
////            for (Map map1 : schemaFields) {
////                String field = (String) (map1.get("field"));
////                if (field.equalsIgnoreCase(fields[i])) {
////                    if (map1.get("name") == null || "".equals(map1.get("name"))) {
////
////                        String type = (String) map1.get("type");
////
////                        if (type.contains("int")) {
////                            map.put(fields[i], Long.parseLong(values[i]));
////                        } else if (type.contains("float")) {
////                            map.put(fields[i], Double.parseDouble(values[i]));
////                        } else if (type.contains("bytes")) {
////                            // 待定
////                            map.put(fields[i], values[i]);
////                        }else {
////                            map.put(fields[i], values[i]);
////                        }
////                    }else {
////                        String name = (String) map1.get("name");
////
////                        if (name.contains("Timestamp")){
////                            // 待定
////                            map.put(fields[i], values[i]);
////                        }
////                    }
////                }
////
////            }
//
//        }


        HashMap<Object, Object> map2 = new HashMap<>();
        map2.put("schema", schema);
        map2.put("payload", map);

        System.out.println(map2);
        String s = JSONUtil.toJSONString(map2);
        System.out.println(s);
        return s;
    }
}
