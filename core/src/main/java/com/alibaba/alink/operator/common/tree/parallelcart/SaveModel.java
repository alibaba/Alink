package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExecutorUtils;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class SaveModel extends CompleteResultFunction {
	public final static ParamInfo <Double> GBDT_Y_PERIOD = ParamInfoFactory
		.createParamInfo("gbdt.y.period", Double.class)
		.setDescription("gbdt.y.period")
		.setRequired()
		.build();
	private static final long serialVersionUID = -8724436969717542401L;
	private Params params;

	public SaveModel(Params params) {
		this.params = params;
	}

	@Override
	public List <Row> calc(ComContext context) {
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);

		if (boostingObjs.executorService != null) {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, boostingObjs.executorService);
		}

		if (context.getTaskId() != 0) {
			return null;
		}

		params.set(GBDT_Y_PERIOD, boostingObjs.prior);

		HistogramBaseTreeObjs tree = context.getObj("tree");

		List <Row> stringIndexerModel = context.getObj("stringIndexerModel");
		List <Object[]> labelsList = context.getObj("labels");

		return TreeModelDataConverter.saveModelWithData(
			tree.roots,
			params,
			stringIndexerModel,
			labelsList == null || labelsList.isEmpty() ? null : labelsList.get(0)
		);
	}
}
