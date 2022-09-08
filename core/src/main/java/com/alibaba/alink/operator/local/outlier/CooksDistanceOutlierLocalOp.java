package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.CooksDistanceDetector;
import com.alibaba.alink.params.outlier.CooksDistanceDetectorParams;

@NameCn("Cook距离异常检测")
@NameEn("Cook's Outlier")
public class CooksDistanceOutlierLocalOp extends BaseOutlierLocalOp <CooksDistanceOutlierLocalOp>
	implements CooksDistanceDetectorParams <CooksDistanceOutlierLocalOp> {
	public CooksDistanceOutlierLocalOp() {
		this(null);
	}

	public CooksDistanceOutlierLocalOp(Params params) {
		super(CooksDistanceDetector::new, params);
	}
}