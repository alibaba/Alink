package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasLifeCycleDefaultAsNeg1;
import com.alibaba.alink.params.io.shared.HasOutputTableName;

public interface OdpsSinkParams<T> extends
	OdpsCatalogParams <T>,
	HasOutputTableName <T>,
	HasLifeCycleDefaultAsNeg1 <T> {

	@NameCn("分区名")
	@DescCn("例如：ds=20190729/dt=12")
	ParamInfo <String> PARTITION = ParamInfoFactory
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

	@NameCn("是否覆写已有数据")
	@DescCn(
		"是否覆写已有数据。若此选项为true，那么当：1）若输出的表已经存在，且不是分区表，那么整个表的数据会被清除；2） 若输出的表已经存在，且是分区表，那么由'分区名'指定的分区的数据会被清除，其它分区的数据不会被清除。若此选项为false，那么当：1）若输出的表已经存在且不是分区表，则报错；2） 若输出的表已存在，且是分区表，且'分区名'指定的分区已存在，则报错")
	ParamInfo <Boolean> OVERWRITE_SINK = ParamInfoFactory
		.createParamInfo("overwriteSink", Boolean.class)
		.setDescription("Whether to overwrite existing data.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getOverwriteSink() {
		return get(OVERWRITE_SINK);
	}

	default T setOverwriteSink(Boolean value) {
		return set(OVERWRITE_SINK, value);
	}
}


