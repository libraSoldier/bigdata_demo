package com.liangyu.hdfs.base;

import com.liangyu.hdfs.HdfsApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

/**
 * @Author liangyu
 * @create 2020/3/27 3:08 下午
 * @Description
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = HdfsApplication.class)
@WebAppConfiguration
@Slf4j
public class BaseTest {
}
