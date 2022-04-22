package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;

@NameCn("DBSCAN序列异常检测")
@NameEn("DBSCAN Series Outlier")
public class DbscanOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <DbscanOutlier4GroupedDataStreamOp>
	implements DbscanDetectorParams <DbscanOutlier4GroupedDataStreamOp> {

	public DbscanOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public DbscanOutlier4GroupedDataStreamOp(Params params) {
		super(DbscanDetector::new, params);
	}
}

