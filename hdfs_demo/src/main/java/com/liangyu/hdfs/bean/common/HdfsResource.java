package com.liangyu.hdfs.bean.common;

import lombok.Data;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * @Author liangyu
 * @create 2020/3/27 2:58 下午
 * @Description 封装HDFS资源信息
 */
@Data
public class HdfsResource {
    /** 资源路径 */
    private String path;
    /** 格式化后的资源大小 */
    private String size;
    /** 真实资源大小 */
    private Long length;
    /** 资源类型 */
    private String type;
    /** 副本系数 */
    private short replication;
    /** 数据块大小 */
    private long blocklength;
    /** 格式化后的数据块大小 */
    private String blockSize;
    /** 修改时间 */
    private String modificationTime;
    /** 访问时间 */
    private String accessTime;
    /** 资源权限 */
    private FsPermission permission;
    /** 所属者 */
    private String owner;
    /** 所属组 */
    private String group;
}
