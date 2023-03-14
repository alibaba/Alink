package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;

@Internal
@NameCn("流式时序分解检测")
public class TimeSeriesDecomposeDetectStreamOp extends BaseOutlierStreamOp <TimeSeriesDecomposeDetectStreamOp>
	implements TimeSeriesDecomposeParams <TimeSeriesDecomposeDetectStreamOp> {
	public TimeSeriesDecomposeDetectStreamOp() {
		this(null);
	}

	public TimeSeriesDecomposeDetectStreamOp(Params params) {
		super(TimeSeriesDecomposeDetector::new, params);
	}
}
