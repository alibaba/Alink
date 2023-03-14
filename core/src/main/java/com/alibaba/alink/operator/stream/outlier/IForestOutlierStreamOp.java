package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;

@NameCn("IForest异常检测")
@NameEn("IForest outlier")
public class IForestOutlierStreamOp extends BaseOutlierStreamOp <IForestOutlierStreamOp>
	implements IForestDetectorParams <IForestOutlierStreamOp> {

	public IForestOutlierStreamOp() {
		this(null);
	}

	public IForestOutlierStreamOp(Params params) {
		super(IForestDetector::new, params);
	}

}
