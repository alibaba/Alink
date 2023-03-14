package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.dl.BaseDLBatchOp;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.params.tensorflow.TensorFlowParams;

import java.util.Collections;

/**
 * A general batch op to run custom TensorFlow (version 2.3.1) scripts for batch datasets.
 * Any number of outputs are allowed from TF scripts, even no outputs.
 */
@NameCn("TensorFlow2自定义脚本")
@NameEn("TensorFlow2 Script")
public class TensorFlow2BatchOp extends BaseDLBatchOp <TensorFlow2BatchOp> implements
	TensorFlowParams <TensorFlow2BatchOp> {

	public TensorFlow2BatchOp() {
		this(new Params());
	}

	public TensorFlow2BatchOp(Params params) {
		super(params);
	}

	@Override
	protected void initDLSystemParams() {
		resPyFiles = Collections.singletonList("res:///entries/tf2_batch_entry.py");
		mainScriptFileName = "res:///entries/tf2_batch_entry.py";
		userScriptMainFileName = "tf_user_main.py";
		numPss = getNumPSs();
		version = Version.TF231;
	}
}
