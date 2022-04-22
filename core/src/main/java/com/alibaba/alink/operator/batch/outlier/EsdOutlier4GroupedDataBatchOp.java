package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD序列异常检测")
@NameEn("ESD Series Outlier")
/**
 * ESD outlier for series data.
 */
public class EsdOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <EsdOutlier4GroupedDataBatchOp>
	implements EsdDetectorParams <EsdOutlier4GroupedDataBatchOp> {

	public EsdOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public EsdOutlier4GroupedDataBatchOp(Params params) {
		super(EsdDetector::new, params);
	}

}
