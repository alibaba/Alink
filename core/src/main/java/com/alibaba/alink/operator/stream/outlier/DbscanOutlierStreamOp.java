package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.operator.common.outlier.DbscanDetectorParams;

/**
 * Density-Based Spatial Clustering used for outlier detection
 */
public class DbscanOutlierStreamOp extends BaseOutlierStreamOp <DbscanOutlierStreamOp>
	implements DbscanDetectorParams <DbscanOutlierStreamOp> {

	public DbscanOutlierStreamOp() {
		this(null);
	}

	public DbscanOutlierStreamOp(Params params) {
		super(DbscanDetector::new, params);
	}
}
