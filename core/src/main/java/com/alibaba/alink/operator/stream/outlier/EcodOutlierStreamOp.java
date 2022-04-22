package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.params.outlier.CopodDetectorParams;
import com.alibaba.alink.operator.common.outlier.EcodDetector;

@NameCn("ECOD异常检测")
@NameEn("ECOD Outlier")
public class EcodOutlierStreamOp extends BaseOutlierStreamOp <EcodOutlierStreamOp>
	implements CopodDetectorParams <EcodOutlierStreamOp> {

	public EcodOutlierStreamOp() {this(null);}

	public EcodOutlierStreamOp(Params params) {
		super(EcodDetector::new, params);
	}
}
