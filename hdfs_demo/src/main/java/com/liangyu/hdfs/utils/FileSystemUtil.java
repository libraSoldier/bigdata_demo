package com.liangyu.hdfs.utils;

import com.liangyu.hdfs.config.HdfsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author liangyu
 * @create 2020/3/27 2:14 下午
 * @Description
 */
@Slf4j
@Component
public class FileSystemUtil {

    private static HdfsConfig hdfsConfig;

    @Autowired
    public void setHdfsConfig(HdfsConfig hdfsConfig){
        FileSystemUtil.hdfsConfig = hdfsConfig;
    }

    /** HDFS文件系统的操作对象 */
    private static FileSystem fileSystem = null;

    public static FileSystem getFileSystemSingleton() throws URISyntaxException, IOException, InterruptedException {

        if (null != fileSystem) { return fileSystem; }
        log.info("开始初始化FileSystem");
        /** HDFS文件系统配置对象 */
        Configuration configuration = new Configuration();
        fileSystem =  FileSystem.get(new URI(hdfsConfig.getPath()), configuration, hdfsConfig.getUser());
        log.info("FileSystem初始化完成");
        return fileSystem;
    }

    /**
     * 格式化资源大小
     * @param length 真实资源大小
     * @return
     */
    public static String formatFileSize(long length) {
        String result = null;
        int sub_string = 0;
        if (length >= 1073741824) {
            sub_string = String.valueOf((float) length / 1073741824).indexOf(
                    ".");
            result = ((float) length / 1073741824 + "000").substring(0,
                    sub_string + 3) + "GB";
        } else if (length >= 1048576) {
            sub_string = String.valueOf((float) length / 1048576).indexOf(".");
            result = ((float) length / 1048576 + "000").substring(0,
                    sub_string + 3) + "MB";
        } else if (length >= 1024) {
            sub_string = String.valueOf((float) length / 1024).indexOf(".");
            result = ((float) length / 1024 + "000").substring(0,
                    sub_string + 3) + "KB";
        } else if (length < 1024) {
            result = Long.toString(length) + "B";
        }

        return result;
    }


}
