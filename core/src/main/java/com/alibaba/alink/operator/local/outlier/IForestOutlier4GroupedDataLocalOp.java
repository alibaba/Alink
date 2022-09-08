package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;

@NameCn("IForest序列异常检测")
@NameEn("IForest Series Outlier")
public class IForestOutlier4GroupedDataLocalOp
	extends BaseOutlier4GroupedDataLocalOp <IForestOutlier4GroupedDataLocalOp>
	implements IForestDetectorParams <IForestOutlier4GroupedDataLocalOp> {

	public IForestOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public IForestOutlier4GroupedDataLocalOp(Params params) {
		super(IForestDetector::new, params);
	}

}
