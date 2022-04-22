package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD序列异常检测")
@NameEn("ESD Series Outlier")
/**
 * ESD outlier for series data.
 */
public class EsdOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <EsdOutlier4GroupedDataStreamOp>
	implements EsdDetectorParams <EsdOutlier4GroupedDataStreamOp> {

	public EsdOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public EsdOutlier4GroupedDataStreamOp(Params params) {
		super(EsdDetector::new, params);
	}

}