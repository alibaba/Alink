package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.operator.common.outlier.DbscanDetectorParams;

public class DbscanOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <DbscanOutlier4GroupedDataStreamOp>
	implements DbscanDetectorParams <DbscanOutlier4GroupedDataStreamOp> {

	public DbscanOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public DbscanOutlier4GroupedDataStreamOp(Params params) {
		super(DbscanDetector::new, params);
	}
}

