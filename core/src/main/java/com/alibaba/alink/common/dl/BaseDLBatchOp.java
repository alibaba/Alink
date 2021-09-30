package com.alibaba.alink.common.dl;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dl.BaseDLParams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for executing deep learning tasks (e.g., tensorflow, pytorch). Note that BaseDLBatchOp is the most
 * powerful op for executing dl-ops and the tasks could know each other. In other words, in this op,: (1) we can write
 * anything back to Alink (2) each task know ips and ports of others, that is we can do distributed training tasks.
 */
public abstract class BaseDLBatchOp<T extends BaseDLBatchOp<T>> extends BatchOperator <T>
	implements BaseDLParams <T> {

	public BaseDLBatchOp() {
		this(new Params());
	}

	public BaseDLBatchOp(Params params) {
		super(params);
	}

	/**
	 * subclasses must implement this method to override the following values:
	 * 		resPyFiles,
	 * 		mainScriptFileName,
	 * 		numPss,
	 * 		userScriptMainFileName
	 */
	abstract protected void initDLSystemParams();

	// all DL system-related files
	protected List <String> resPyFiles;
	// `mainScriptFileName` and `entryFuncName` are the main file and its entrypoint provided by our wrapping
	protected String mainScriptFileName;
	protected Integer numPss = null;
	protected String userScriptMainFileName;

	// entry function
	private static final String entryFuncName = "entry_func";
	// User's main script is renamed to `userScriptMainFileName`, and `main` is called

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		initDLSystemParams();
		BatchOperator <?> input = inputs[0];

		if (null != getSelectedCols()) {
			input = input.select(getSelectedCols());
		}

		Map <String, String> algoParams = new HashMap <>();
		String userParamsStr = getUserParams();
		if (StringUtils.isNoneEmpty(userParamsStr)) {
			TypeToken <Map <String, String>> typeToken = new TypeToken <Map <String, String>>() {};
			Map <String, String> userParams = new Gson().fromJson(userParamsStr, typeToken.getType());
			userParams.forEach(algoParams::put);
		}

		ExternalFilesConfig externalFilesConfig = getUserFiles()
			.addFilePaths(resPyFiles)
			.addRenameMap(getMainScriptFile(), userScriptMainFileName);

		DLLauncherBatchOp dlLauncherBatchOp = new DLLauncherBatchOp()
			.setOutputSchemaStr(getOutputSchemaStr())
			.setNumWorkers(getNumWorkers())
			.setNumPSs(numPss)
			.setEntryFunc(entryFuncName)
			.setPythonEnv(getPythonEnv())
			.setUserFiles(externalFilesConfig.toJson())
			.setScript(mainScriptFileName)
			.setUserDefinedParams(JsonConverter.toJson(algoParams))
			.setIntraOpParallelism(getIntraOpParallelism())
			.setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?>[] tfInputs = new BatchOperator <?>[inputs.length];
		tfInputs[0] = input;
		System.arraycopy(inputs, 1, tfInputs, 1, inputs.length - 1);
		BatchOperator <?> tfModel = dlLauncherBatchOp.linkFrom(tfInputs);
		this.setOutputTable(tfModel.getOutputTable());
		return (T) this;
	}
}
