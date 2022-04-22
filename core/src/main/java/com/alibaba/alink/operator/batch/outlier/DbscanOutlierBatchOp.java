package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;

@NameCn("DBSCAN异常检测")
@NameEn("DBSCAN Outlier")
public class DbscanOutlierBatchOp extends BaseOutlierBatchOp <DbscanOutlierBatchOp>
	implements DbscanDetectorParams <DbscanOutlierBatchOp> {
	public DbscanOutlierBatchOp() {
		this(null);
	}

	public DbscanOutlierBatchOp(Params params) {
		super(DbscanDetector::new, params);
	}
}