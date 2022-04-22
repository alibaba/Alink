package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetector;
import com.alibaba.alink.params.outlier.BoxPlotDetectorParams;

@NameCn("BoxPlot序列异常检测")
@NameEn("BoxPlot Series Outlier")
/**
 * BoxPlot outlier for series data.
 */
public class BoxPlotOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <BoxPlotOutlier4GroupedDataStreamOp>
	implements BoxPlotDetectorParams <BoxPlotOutlier4GroupedDataStreamOp> {

	public BoxPlotOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public BoxPlotOutlier4GroupedDataStreamOp(Params params) {
		super(BoxPlotDetector::new, params);
	}

}
