package com.liangyu.hive.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * @Author liangyu
 * @create 2020/3/27 5:05 下午
 * @Description
 */
@Slf4j
public class HiveConnectUtil {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static Connection conn = null;
//    private static PreparedStatement stmt = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    private static String url = "jdbc:hive2://127.0.0.1:10000/default";
    private static String user = "liangyu";
    private static String password = "";

    /**
     * 初始化hive连接
     * @throws Exception
     */
    public static void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
//        stmt = (PreparedStatement) conn.createStatement();
        stmt = conn.createStatement();
    }

    /**
     * 释放资源
     * @throws Exception
     */
    public static void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * 创建数据库
     * @param databaseName
     * @throws Exception
     */
    public static void createHiveDatabase(String databaseName) throws Exception {
        init();
        String sql = "create database " + databaseName;
        log.info("Running：" + sql);
        stmt.execute(sql);
        destory();
    }

    /**
     * 查询所有数据库
     * @throws Exception
     */
    public static List<String> showHiveDatabases() throws Exception {
        init();
        String sql = "show databases";
        log.info("Running：" + sql);
        rs = stmt.executeQuery(sql);

        List<String> resultList = new ArrayList<>();
        while (rs.next()) {
            resultList.add(rs.getString(1));
        }
        destory();
        return resultList;
    }

    /**
     * 创建表
     * @param sql
     * @throws Exception
     */
    public static void createHiveTable(String sql) throws Exception {
        init();
        log.info("Running: " + sql);
        stmt.execute(sql);
        destory();
    }

    /**
     * 查询所有表
     * @throws Exception
     */
    public static List<String> showHiveTables() throws Exception {
        String sql = "show tables";
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);

        List<String> resultList = new ArrayList<>();
        while (rs.next()) {
            resultList.add(rs.getString(1));
        }
        destory();
        return resultList;
    }

    /**
     * 查看表结构
     * @param tableName
     * @throws Exception
     */
    public static List<String> descHiveTable(String tableName) throws Exception {
        String sql = "desc "+tableName;
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        List<String> resultList = new ArrayList<>();
        while (rs.next()) {
            resultList.add(rs.getString(1) + "\t" + rs.getString(2));
        }
        destory();
        return resultList;
    }

    /**
     * 加载数据(导入本地文件)
     * @param tableName
     * @param filePath
     * @throws Exception
     */
    public static void loadData(String tableName,String filePath) throws Exception {
        String sql = "load data local inpath '" + filePath + "' overwrite into table "+tableName;
        log.info("Running: " + sql);
        stmt.execute(sql);
        destory();
    }

    /**
     * 查询数据
     * @param tableName
     * @param columnLable
     * @throws Exception
     */
    public static void selectData(String tableName,List<String> columnLable) throws Exception {
        String sql = "select * from "+tableName;
        log.info("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < columnLable.size(); i++) {
                sb.append(rs.getString(columnLable.get(i))+"\t");
            }
            System.out.println(sb.toString());
        }
        destory();
    }

    /**
     * 统计查询（会运行mapreduce作业）
     * @param tableName
     * @throws Exception
     */
    public static void countData(String tableName) throws Exception {
        String sql = "select count(1) from "+tableName;
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
        destory();
    }

    /**
     * 删除数据库
     * @param databaseName
     * @throws Exception
     */
    public static void dropHiveDatabase(String databaseName) throws Exception {
        String sql = "drop database if exists "+databaseName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
        destory();
    }

    /**
     * 删除数据库表
     * @param tableName
     * @throws Exception
     */
    public static void deopHiveTable(String tableName) throws Exception {
        String sql = "drop table if exists "+tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
        destory();
    }

    /**
     * 查看表是否存在
     * @param database
     * @param tableName
     * @return
     */
    public static boolean existHiveTable(String database, String tableName) throws Exception{

        boolean result = false;
        String hql = "SHOW TABLES IN " + database;

        rs = stmt.executeQuery(hql);
        while (rs.next())
        {
            if (rs.getString(1).trim().toUpperCase().equals(tableName.toUpperCase()))
            {
                result = true;
                break;
            }
        }
        destory();
        return result;
    }


    /**
     * 获取Partitions
     * @param tablelName
     * @return
     * @throws Exception
     */
    public static List<String[]> getHiveTblPartitions(String tablelName) throws Exception{
        List<String[]> list = null;
        String[] item = null;

        rs = stmt.executeQuery("desc " + tablelName);
        list = new LinkedList<String[]>();

        while (rs.next()) {
            if (rs.getString(1).equals("# Partition Information")) {
                while (rs.next()) {
                    if (rs.getString(1).trim().equals("# col_name")) {
                        continue;
                    }

                    if ("".equals(rs.getString(1).trim())) {
                        continue;
                    }
                    String column = rs.getString(1).trim().toUpperCase();
                    String type = rs.getString(2).trim().toUpperCase();
                    String comment = "";
                    if (rs.getString(3) != null && rs.getString(3).trim().length() > 0) {
                        comment = rs.getString(3).trim().toUpperCase();
                        if ("NONE".equals(comment)) {
                            comment = "";
                        }
                    }
                    item = new String[] { column, type, comment };
                    list.add(item);
                }
            }
        }
        destory();
        return list;

    }


    /**
     * 获取表的字段
     * @param tableName
     * @return
     * @throws Exception
     */
    public static List<String> getHiveTblColumns(String tableName) throws Exception{

        List<String> list = null;

        rs = stmt.executeQuery("desc formatted " + tableName);
        list = new LinkedList<String>();
        while (rs.next()) {
            if (rs.getString(1).trim().equals("# col_name")){
                continue;
            }

            if (rs.getString(1).equals("# Detailed Table Information") || rs.getString(1).equals("# Partition Information")) {
                break;
            }

            if (rs.getString(1).trim().equals("")) {
                continue;
            }
            System.out.println(rs.getString(1).trim());
            list.add(rs.getString(1).trim().toUpperCase());
        }

        destory();
        return list;
    }


    /**
     *获取表所在的路径
     * @param tablelName
     * @return
     * @throws Exception
     */
    public static String getHiveTblPath(String tablelName) throws Exception {
        String result = "";
        rs = stmt.executeQuery("desc extended " + tablelName);
        while (rs.next()) {
            if(rs.getString(1).trim().equals("Detailed Table Information")) {
                String content = rs.getString(2).trim();
                int start = content.indexOf("location:");
                if (start == -1) {
                    continue;
                }

                String sub = content.substring(start);
                int end = sub.indexOf(",");
                if (end == -1) {
                    continue;
                }

                result = sub.substring("location:".length(), end);
            } else {
                continue;
            }

        }

        return result;
    }


    /**
     * 获取表所在的路径
     * @param tablelName
     * @return
     * @throws Exception
     */
    public static Map<String,String> getNewHiveTblPath(String tablelName) throws Exception {
        String result = "";
        String field = "";
        Map<String,String> map = new HashMap<String,String>();
        rs = stmt.executeQuery("desc extended " + tablelName);
        while (rs.next()) {
            if(rs.getString(1).trim().equals("Detailed Table Information")) {
                String content = rs.getString(2).trim();
                field=getField(content);
                int start = content.indexOf("dragoncluster");
                if (start == -1) {
                    continue;
                }

                String sub = content.substring(start);
                int end = sub.indexOf(",");
                if (end == -1){
                    continue;
                }

                result = sub.substring("dragoncluster".length(), end);
            } else {
                continue;
            }

            // String content = res.getString(1).trim();

        }

        map.put("field", field);
        map.put("hdfsPath", result);
        destory();
        return map;
    }


    /**
     * desc:获取hive表文件的类型
     * @param tablelName
     * @return
     */
    public static String getHiveTableFileType(String tablelName) throws Exception{
        String result = null;
        rs = stmt.executeQuery("desc extended " + tablelName);
        while (rs.next()) {
            if(rs.getString(1).trim().equals("Detailed Table Information"))
            {
                String content = rs.getString(2).trim();
                if(content.toUpperCase().contains("TEXTINPUTFORMAT")) {
                    result = "TEXTFILE";
                } else if(content.toUpperCase().contains("SEQUENCEFILEINPUTFORMAT")) {
                    result = "SEQUENCEFILE";
                } else {
                    result = "SEQUENCEFILE";
                }
            }
        }
        destory();
        return result;
    }


    public static String getField(String content) throws Exception{
        int start = content.indexOf("field.delim=");
        if(start==-1){
            return "\\\\001";
        }else{
            String sub = content.substring(start);
            return sub.substring("field.delim=".length(), "field.delim=".length()+1);

        }
    }



}
