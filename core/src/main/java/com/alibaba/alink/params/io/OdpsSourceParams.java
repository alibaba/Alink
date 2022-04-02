package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasInputTableName;

public interface OdpsSourceParams<T> extends WithParams<T>,
    OdpsCatalogParams<T>,
    HasInputTableName<T> {

    @NameCn("分区名")
    @DescCn(
        "1)单级、单个分区示例：ds=20190729；2)多级分区之间用\" / \"分隔，例如：ds=20190729/dt=12； 3)多个分区之间用\", \"分隔，例如：ds=20190729,ds=20190730")
    ParamInfo<String> PARTITIONS = ParamInfoFactory
        .createParamInfo("partitions", String.class)
        .setDescription("partitions")
        .setHasDefaultValue(null)
        .build();

    default String getPartitions() {
        return get(PARTITIONS);
    }

    default T setPartitions(String value) {
        return set(PARTITIONS, value);
    }
}
