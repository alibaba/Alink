package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.featuregenerator.HasWindowTime;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import com.alibaba.alink.params.shared.colname.HasTimeColDefaultAsNull;

public interface Export2FileSinkParams<T> extends WithParams <T>,
	HasTimeColDefaultAsNull <T>,
	HasFilePath <T>,
	HasOverwriteSink <T>,
	HasWindowTime <T> {

	@NameCn("分区格式化字符串")
	@DescCn("可以使用类似于 year=yyyy/month=MM/day=dd 的形式自定义分区格式")
	ParamInfo <String> PARTITIONS_FORMAT = ParamInfoFactory
		.createParamInfo("partitionsFormat", String.class)
		.setDescription("Partitions format. year=yyyy/month=MM/day=dd, day=yyyyMMdd etc...")
		.setHasDefaultValue(null)
		.build();

	default T setPartitionsFormat(String partitionFormat) {
		return set(PARTITIONS_FORMAT, partitionFormat);
	}

	default String getPartitionsFormat() {
		return get(PARTITIONS_FORMAT);
	}
}
