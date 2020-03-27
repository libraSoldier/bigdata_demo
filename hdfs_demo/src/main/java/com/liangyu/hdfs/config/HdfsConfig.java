package com.liangyu.hdfs.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @Author liangyu
 * @create 2020/3/27 2:30 下午
 * @Description
 */
@Data
@Component
@Configuration
@ConfigurationProperties(value = "hdfs")
public class HdfsConfig {

    /** HDFS文件系统服务器的地址以及端口 */
    private String path;

    /** HDFS文件系统服务器的地址以及端口 */
    private String user;
}
