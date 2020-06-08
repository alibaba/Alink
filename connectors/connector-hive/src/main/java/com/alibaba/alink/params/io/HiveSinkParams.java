package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.shared_params.HasOutputTableName;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HiveSinkParams<T> extends WithParams<T>,
    HiveDBParams<T>,
    HasOutputTableName<T>, HasOverwriteSink<T> {
    /**
     * @cn-name 分区名
     * @cn 例如：ds=20190729/dt=12
     */
    ParamInfo<String> PARTITION = ParamInfoFactory
        .createParamInfo("partition", String.class)
        .setDescription("partition")
        .setHasDefaultValue(null)
        .build();

    default String getPartition() {
        return get(PARTITION);
    }

    default T setPartition(String value) {
        return set(PARTITION, value);
    }

//    /**
//     * @cn-name 表是否有动态分区列
//     * @cn 表是否有动态分区列
//     */
//    ParamInfo<Boolean> HAS_DYN_PART_COLS = ParamInfoFactory
//        .createParamInfo("hasDynPartCols", Boolean.class)
//        .setDescription("")
//        .setHasDefaultValue(false)
//        .build();
//
//    default T setHasDynPartCols(Boolean value) {
//        return set(HAS_DYN_PART_COLS, value);
//    }

}