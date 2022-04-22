package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;

@NameCn("IForest序列异常检测")
@NameEn("IForest Series Outlier")
public class IForestOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <IForestOutlier4GroupedDataBatchOp>
	implements IForestDetectorParams <IForestOutlier4GroupedDataBatchOp> {

	public IForestOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public IForestOutlier4GroupedDataBatchOp(Params params) {
		super(IForestDetector::new, params);
	}

}
