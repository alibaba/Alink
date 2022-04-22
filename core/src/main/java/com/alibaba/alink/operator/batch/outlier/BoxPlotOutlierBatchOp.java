package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetector;
import com.alibaba.alink.params.outlier.BoxPlotDetectorParams;

@NameCn("BoxPlot异常检测")
@NameEn("BoxPlot Outlier")
/**
 * BoxPlot outlier for tuple data.
 */
public class BoxPlotOutlierBatchOp extends BaseOutlierBatchOp <BoxPlotOutlierBatchOp>
	implements BoxPlotDetectorParams <BoxPlotOutlierBatchOp> {

	public BoxPlotOutlierBatchOp() {
		this(null);
	}

	public BoxPlotOutlierBatchOp(Params params) {
		super(BoxPlotDetector::new, params);
	}

}
