package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.DynamicTimeWarpingDetector;
import com.alibaba.alink.params.outlier.DynamicTimeWarpingDetectorParams;

@NameCn("DTW异常检测")
@NameEn("DTW Outlier")
public class DynamicTimeWarpOutlierStreamOp extends BaseOutlierStreamOp <DynamicTimeWarpOutlierStreamOp>
	implements DynamicTimeWarpingDetectorParams <DynamicTimeWarpOutlierStreamOp> {

	public DynamicTimeWarpOutlierStreamOp() {this(null);}

	public DynamicTimeWarpOutlierStreamOp(Params params) {
		super(DynamicTimeWarpingDetector::new, params);
	}
}
