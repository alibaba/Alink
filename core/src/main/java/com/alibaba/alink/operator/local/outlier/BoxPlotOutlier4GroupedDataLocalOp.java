package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetector;
import com.alibaba.alink.params.outlier.BoxPlotDetectorParams;

@NameCn("BoxPlot序列异常检测")
@NameEn("BoxPlot Series Outlier")
/**
 * BoxPlot outlier for series data.
 */
public class BoxPlotOutlier4GroupedDataLocalOp
	extends BaseOutlier4GroupedDataLocalOp <BoxPlotOutlier4GroupedDataLocalOp>
	implements BoxPlotDetectorParams <BoxPlotOutlier4GroupedDataLocalOp> {

	public BoxPlotOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public BoxPlotOutlier4GroupedDataLocalOp(Params params) {
		super(BoxPlotDetector::new, params);
	}

}
