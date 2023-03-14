package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.dl.BaseDLBatchOp;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.params.tensorflow.TensorFlowParams;

import java.util.Collections;

/**
 * A general batch op to run custom TensorFlow (version 1.15) scripts for batch datasets.
 * Any number of outputs are allowed from TF scripts, even no outputs.
 */
@NameCn("TensorFlow自定义脚本")
@NameEn("TensorFlow Script")
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
		version = Version.TF115;
	}
}
