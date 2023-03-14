package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.shared.colname.HasTimeCol;

@Internal
@NameCn("批式时序分解检测")
public class TimeSeriesDecomposeDetectBatchOp extends BaseOutlierBatchOp <TimeSeriesDecomposeDetectBatchOp>
	implements TimeSeriesDecomposeParams <TimeSeriesDecomposeDetectBatchOp>,
	HasTimeCol <TimeSeriesDecomposeDetectBatchOp> {

	public TimeSeriesDecomposeDetectBatchOp() {
		this(null);
	}

	public TimeSeriesDecomposeDetectBatchOp(Params params) {
		super(TimeSeriesDecomposeDetector::new, params);
	}
}
