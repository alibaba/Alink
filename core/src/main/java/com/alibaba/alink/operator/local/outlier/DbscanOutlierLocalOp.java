package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.DbscanDetector;
import com.alibaba.alink.params.outlier.DbscanDetectorParams;

@NameCn("DBSCAN异常检测")
@NameEn("DBSCAN Outlier")
public class DbscanOutlierLocalOp extends BaseOutlierLocalOp <DbscanOutlierLocalOp>
	implements DbscanDetectorParams <DbscanOutlierLocalOp> {
	public DbscanOutlierLocalOp() {
		this(null);
	}

	public DbscanOutlierLocalOp(Params params) {
		super(DbscanDetector::new, params);
	}
}