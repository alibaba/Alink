package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.finance.BinningPredictParams;
import com.alibaba.alink.params.finance.BinningTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Generate the efficients for bucket random projection.
 * The efficients are further used to hash vector.
 */
@NameCn("分箱")
public class Binning extends Trainer <Binning, BinningModel>
	implements BinningTrainParams <Binning>,
	BinningPredictParams <Binning>,
	HasLazyPrintModelInfo <Binning> {
	private static final long serialVersionUID = 5290580581431785782L;

	public Binning() {
		super(new Params());
	}

	public Binning(Params params) {
		super(params);
	}

}
