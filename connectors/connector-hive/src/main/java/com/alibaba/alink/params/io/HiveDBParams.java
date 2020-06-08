package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HiveDBParams<T> extends WithParams<T> {
    /**
     * @cn Hive配置文件路径。本地路径或hdfs路径，路径里需包括hive-site.xml文件。示例：hdfs://192.168.99.102:9000/hive-2.0.1/conf
     * @cn-name Hive配置文件路径
     */
    ParamInfo<String> HIVE_CONF_DIR = ParamInfoFactory
        .createParamInfo("hiveConfDir", String.class)
        .setDescription("Hive configuration directory")
        .setRequired()
        .build();

    default T setHiveConfDir(String value) {
        return set(HIVE_CONF_DIR, value);
    }

    /**
     * @cn hive版本号
     * @cn-name hive版本号
     */
    ParamInfo<String> HIVE_VERSION = ParamInfoFactory
        .createParamInfo("hiveVersion", String.class)
        .setDescription("Hive version number")
        .setRequired()
        .build();

    default T setHiveVersion(String value) {
        return set(HIVE_VERSION, value);
    }

    /**
     * @cn hive数据库名字
     * @cn-name hive数据库名字
     */
    ParamInfo<String> DB_NAME = ParamInfoFactory
        .createParamInfo("dbName", String.class)
        .setDescription("Hive database name")
        .setRequired()
        .build();

    default T setDbName(String value) {
        return set(DB_NAME, value);
    }

}