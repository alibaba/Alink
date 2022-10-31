package com.alibaba.alink.common.dl;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.RebalanceBatchOp;
import com.alibaba.alink.params.dl.BaseDLTableModelTrainParams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for executing deep learning tasks (e.g., tensorflow, pytorch). Note that BaseDLTableModelTrainBatchOp is a
 * restricted version of BaseDLBatchOp. That is, in this class, we still know ips and ports of all tasks, but we must
 * write table model back to Alink with schema "model_id long, model_info string".
 */
@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.DATA, desc = PortDesc.DL_BC_DATA, isRepeated = true)}
)
@OutputPorts(values = @PortSpec(PortType.MODEL))
public abstract class BaseDLTableModelTrainBatchOp<T extends BaseDLTableModelTrainBatchOp <T>>
	extends BatchOperator <T> implements BaseDLTableModelTrainParams <T> {

	public BaseDLTableModelTrainBatchOp() {
		this(new Params());
	}

	public BaseDLTableModelTrainBatchOp(Params params) {
		super(params);
	}

	/**
	 * subclasses must implement this method to override the following values: resPyFiles, mainScriptFileName, numPss
	 */
	abstract protected void initDLSystemParams();

	// all DL system-related files
	protected List <String> resPyFiles;
	// `mainScriptFileName` and `entryFuncName` are the main file and its entrypoint provided by our wrapping
	protected String mainScriptFileName;
	protected Integer numPss = null;
	protected String userMainScriptRename;
	// when `pythonEnv` not set, download from plugin with `version`
	protected Version version;
	protected Map <String, String> scriptRenameMap = new HashMap <>();

	// entry function
	private static final String entryFuncName = "entry_func";
	// output schema
	private static final String MODEL_SCHEMA_STR = "model_id long, model_info string";

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		initDLSystemParams();
		BatchOperator <?> input = inputs[0];

		if (null != getSelectedCols()) {
			input = input.select(getSelectedCols());
		}

		input = new RebalanceBatchOp().linkFrom(input);

		ExternalFilesConfig externalFiles = getUserFiles()
			.addFilePaths(resPyFiles)
			.addRenameMap(getMainScriptFile(), userMainScriptRename);

		Map <String, String> algoParams = new HashMap <>();
		String userParamsStr = getUserParams();
		if (StringUtils.isNoneEmpty(userParamsStr)) {
			TypeToken <Map <String, String>> typeToken = new TypeToken <Map <String, String>>() {};
			Map <String, String> userParams = new Gson().fromJson(userParamsStr, typeToken.getType());
			userParams.forEach(algoParams::put);
		}

		DLLauncherBatchOp dlLauncherBatchOp = new DLLauncherBatchOp()
			.setOutputSchemaStr(MODEL_SCHEMA_STR)
			.setNumWorkers(getNumWorkers())
			.setNumPSs(numPss)
			.setEntryFunc(entryFuncName)
			.setPythonEnv(getPythonEnv())
			.setEnvVersion(version)
			.setUserFiles(externalFiles)
			.setMainScriptFile(mainScriptFileName)
			.setUserParams(JsonConverter.toJson(algoParams))
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
