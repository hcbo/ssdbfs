package com.hcb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtils {
    public static int getOps(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return Integer.parseInt(myProp.getProperty("ops"));
    }
    public static int getSources(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return Integer.parseInt(myProp.getProperty("sources"));
    }
    public static String getZkServers(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("zkServers");
    }
    public static String getKafkaServers(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("kafkaServers");
    }

    public static String getSparkPartitions(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("sparkPartitions");
    }

    public static String getLogPath(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("logPath");
    }

    public static String getPort(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("port");
    }
    public static String getRedisPort(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("redisPort");
    }

    public static String getIp(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("ip");
    }

    public static String getPassword(){
        // 动态加载操作符个数
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream inStream = cl.getResourceAsStream("hcbConfig.properties");
        Properties myProp = new Properties();
        try {
            myProp.load(inStream);
        } catch (IOException e) {
            MfsFileSystem.LOG.error("hcbConfig");
        }
        return myProp.getProperty("password");
    }



}
