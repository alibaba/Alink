package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BoxPlotDetector;
import com.alibaba.alink.params.outlier.BoxPlotDetectorParams;

@NameCn("BoxPlot异常检测")
@NameEn("BoxPlot Outlier")
/**
 * BoxPlot outlier for tuple data.
 */
public class BoxPlotOutlierLocalOp extends BaseOutlierLocalOp <BoxPlotOutlierLocalOp>
	implements BoxPlotDetectorParams <BoxPlotOutlierLocalOp> {

	public BoxPlotOutlierLocalOp() {
		this(null);
	}

	public BoxPlotOutlierLocalOp(Params params) {
		super(BoxPlotDetector::new, params);
	}

}
