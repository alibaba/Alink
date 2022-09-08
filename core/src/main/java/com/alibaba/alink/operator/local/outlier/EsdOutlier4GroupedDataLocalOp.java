package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD序列异常检测")
@NameEn("ESD Series Outlier")
/**
 * ESD outlier for series data.
 */
public class EsdOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <EsdOutlier4GroupedDataLocalOp>
	implements EsdDetectorParams <EsdOutlier4GroupedDataLocalOp> {

	public EsdOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public EsdOutlier4GroupedDataLocalOp(Params params) {
		super(EsdDetector::new, params);
	}

}
