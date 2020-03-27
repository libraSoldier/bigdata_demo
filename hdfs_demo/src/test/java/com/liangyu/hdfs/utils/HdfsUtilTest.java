package com.liangyu.hdfs.utils;

import com.liangyu.hdfs.base.BaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

/**
 * @Author liangyu
 * @create 2020/3/27 3:07 下午
 * @Description
 */
@Slf4j
public class HdfsUtilTest extends BaseTest {

    @Test
    public void listDirsTest() throws InterruptedException, IOException, URISyntaxException {

        log.info("开始进行单元测试！");
        HdfsUtil.listDirs("/user");
    }

}