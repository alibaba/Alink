package com.alibaba.alink.pipeline.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedData;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.params.outlier.IForestDetectorParams;

@NameCn("IForest序列异常检测")
@NameEn("IForest Series Outlier")
public class IForestOutlier4GroupedData extends BaseOutlier4GroupedData <IForestOutlier4GroupedData>
	implements IForestDetectorParams <IForestOutlier4GroupedData> {

	public IForestOutlier4GroupedData() {
		this(null);
	}

	public IForestOutlier4GroupedData(Params params) {
		super(IForestDetector::new, params);
	}

}
