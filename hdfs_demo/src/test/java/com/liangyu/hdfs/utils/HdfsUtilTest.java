package com.liangyu.hdfs.utils;

import com.liangyu.hdfs.base.BaseTest;
import com.liangyu.hdfs.bean.common.HdfsResource;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @Author liangyu
 * @create 2020/3/27 3:07 下午
 * @Description
 */
@Slf4j
public class HdfsUtilTest extends BaseTest {


    @Test
    public void listDirsTest() throws InterruptedException, IOException, URISyntaxException {

        log.info("*****开始进行单元测试*****");
        List<HdfsResource> demoList =  HdfsUtil.listDirs("/user");
        log.info("单元测试结果为：" + demoList);
    }

}