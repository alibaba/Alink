package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;

@NameCn("DBSCAN序列异常检测")
@NameEn("DBSCAN Series Outlier")
public class DbscanOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <DbscanOutlier4GroupedDataLocalOp>
	implements DbscanDetectorParams <DbscanOutlier4GroupedDataLocalOp> {

	public DbscanOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public DbscanOutlier4GroupedDataLocalOp(Params params) {
		super(DbscanDetector::new, params);
	}
}

