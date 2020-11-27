package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by {@link MultiStringIndexer}.
 */
public class MultiStringIndexerModel extends MapModel <MultiStringIndexerModel>
	implements MultiStringIndexerPredictParams <MultiStringIndexerModel> {

	private static final long serialVersionUID = -1368094041469654922L;

	public MultiStringIndexerModel(Params params) {
		super(MultiStringIndexerModelMapper::new, params);
	}
}