package com.liangyu.hdfs.utils;

import com.liangyu.hdfs.bean.common.HdfsResource;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author liangyu
 * @create 2020/3/27 2:45 下午
 * @Description
 */
public class HdfsUtil {

    public static List<HdfsResource> listDirs(String directorName) throws InterruptedException, IOException, URISyntaxException {

        List<HdfsResource> resultList = new ArrayList<HdfsResource>();

        Path path = new Path(directorName);

        FileStatus[] fileStatuses = FileSystemUtil.getFileSystemSingleton().listStatus(path);

        if(ArrayUtils.isEmpty(fileStatuses)) { return resultList; }

        HdfsResource hdfsResource = null;

        for (FileStatus fileStatus : fileStatuses) {
            hdfsResource = new HdfsResource();
            hdfsResource.setPath(fileStatus.getPath().toString());
            hdfsResource.setLength(fileStatus.getLen());
            hdfsResource.setSize(FileSystemUtil.formatFileSize(fileStatus.getLen()));

            if (fileStatus.isDirectory()) {
                hdfsResource.setType("目录");
            } else {
                hdfsResource.setType("文件");
            }

            hdfsResource.setReplication(fileStatus.getReplication());
            hdfsResource.setBlocklength(fileStatus.getBlockSize());
            hdfsResource.setBlockSize(FileSystemUtil.formatFileSize(fileStatus.getBlockSize()));
            hdfsResource.setModificationTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(fileStatus.getModificationTime())));
            hdfsResource.setAccessTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(fileStatus.getAccessTime())));
            hdfsResource.setPermission(fileStatus.getPermission());
            hdfsResource.setOwner(fileStatus.getOwner());
            hdfsResource.setGroup(fileStatus.getGroup());

            resultList.add(hdfsResource);
        }

        return resultList;
    }
}
