package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetector;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetectorParams;

@NameCn("BoxPlot序列异常检测")
@NameEn("BoxPlot Series Outlier")
/**
 * BoxPlot outlier for series data.
 */
public class BoxPlotOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <BoxPlotOutlier4GroupedDataBatchOp>
	implements BoxPlotDetectorParams <BoxPlotOutlier4GroupedDataBatchOp> {

	public BoxPlotOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public BoxPlotOutlier4GroupedDataBatchOp(Params params) {
		super(BoxPlotDetector::new, params);
	}

}
