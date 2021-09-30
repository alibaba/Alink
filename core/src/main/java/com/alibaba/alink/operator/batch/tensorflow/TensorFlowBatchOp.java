package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.dl.BaseDLBatchOp;
import com.alibaba.alink.common.dl.DLEnvConfig;
import com.alibaba.alink.params.tensorflow.TensorFlowParams;

import java.util.Collections;

public class TensorFlowBatchOp extends BaseDLBatchOp <TensorFlowBatchOp> implements
	TensorFlowParams <TensorFlowBatchOp> {

	public TensorFlowBatchOp() {
		this(new Params());
	}

	public TensorFlowBatchOp(Params params) {
		super(params);
	}

	@Override
	protected void initDLSystemParams() {
		resPyFiles = Collections.singletonList("res:///entries/tf_batch_entry.py");
		mainScriptFileName = "res:///entries/tf_batch_entry.py";
		userScriptMainFileName = "tf_user_main.py";
		numPss = getNumPSs();

		if (StringUtils.isNullOrWhitespaceOnly(getPythonEnv())) {
			setPythonEnv(DLEnvConfig.getTF115DefaultPythonEnv());
		}
	}
}
