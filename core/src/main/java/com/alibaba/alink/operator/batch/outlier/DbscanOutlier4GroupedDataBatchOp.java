package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.operator.common.outlier.DbscanDetectorParams;

public class DbscanOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <DbscanOutlier4GroupedDataBatchOp>
	implements DbscanDetectorParams <DbscanOutlier4GroupedDataBatchOp> {

	public DbscanOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public DbscanOutlier4GroupedDataBatchOp(Params params) {
		super(DbscanDetector::new, params);
	}
}

