package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.feature.featuregenerator.HasWindowTime;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import com.alibaba.alink.params.shared.colname.HasTimeCol;
import com.alibaba.alink.params.shared.colname.HasTimeColDefaultAsNull;

public interface Export2FileSinkParams<T> extends WithParams <T>,
	HasTimeColDefaultAsNull <T>,
	HasFilePath <T>,
	HasOverwriteSink <T>,
	HasWindowTime <T> {
}
