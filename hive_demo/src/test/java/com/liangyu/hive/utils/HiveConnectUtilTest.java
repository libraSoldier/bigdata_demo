package com.liangyu.hive.utils;

import com.liangyu.hive.base.BaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * @Author liangyu
 * @create 2020/3/30 11:49 上午
 * @Description
 */
@Slf4j
public class HiveConnectUtilTest extends BaseTest {

    @Test
    public void createHiveDatabase() throws Exception {
        HiveConnectUtil.createHiveDatabase("test");
    }

    @Test
    public void showHiveDatabases() throws Exception {
        log.info("单测结果" + HiveConnectUtil.showHiveDatabases());
    }
}