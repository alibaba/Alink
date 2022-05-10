package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;

@NameCn("IForest序列异常检测")
@NameEn("IForest Series Outlier")
public class IForestOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <IForestOutlier4GroupedDataStreamOp>
	implements IForestDetectorParams <IForestOutlier4GroupedDataStreamOp> {

	public IForestOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public IForestOutlier4GroupedDataStreamOp(Params params) {
		super(IForestDetector::new, params);
	}

}
