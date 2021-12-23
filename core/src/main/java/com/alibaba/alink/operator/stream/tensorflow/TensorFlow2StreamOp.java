package com.alibaba.alink.operator.stream.tensorflow;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.DLEnvConfig;
import com.alibaba.alink.common.dl.BaseDLStreamOp;
import com.alibaba.alink.params.tensorflow.TensorFlowParams;

import java.util.Collections;

import static com.alibaba.alink.common.dl.utils.DLLauncherUtils.adjustNumWorkersPSs;

/**
 * A general stream op to run custom TensorFlow (version 2.3.1) scripts for stream datasets.
 * By default, the dataset can only be accesses once, unless explicitly saved.
 * Any number of outputs are allowed from TF scripts, even no outputs.
 */
public class TensorFlow2StreamOp extends BaseDLStreamOp <TensorFlow2StreamOp>
	implements TensorFlowParams <TensorFlow2StreamOp> {


	public TensorFlow2StreamOp() {
		this(new Params());
	}

	public TensorFlow2StreamOp(Params params) {
		super(params);
	}

	@Override
	protected void initDLSystemParams() {
		resPyFiles = Collections.singletonList("res:///entries/tf2_stream_entry.py");
		mainScriptFileName = "res:///entries/tf2_stream_entry.py";
		userScriptMainFileName = "tf_user_main.py";

		Tuple2 <Integer, Integer> numWorkersPSsTuple = adjustNumWorkersPSs(getNumWorkers(), getNumPSs(),
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().getParallelism());
		setNumWorkers(numWorkersPSsTuple.f0);
		numPss = numWorkersPSsTuple.f1;

		if (StringUtils.isNullOrWhitespaceOnly(getPythonEnv())) {
			setPythonEnv(DLEnvConfig.getTF231DefaultPythonEnv());
		}
	}
}