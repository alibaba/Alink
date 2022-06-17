package com.alibaba.alink.common.dl;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.params.dl.HasUserFiles;
import com.alibaba.alink.params.tensorflow.bert.EasyTransferConfigTrainParams;
import com.google.gson.reflect.TypeToken;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.MODEL))
@ParamSelectColumnSpec(name = "selectedCols")
@Internal
public class EasyTransferConfigTrainBatchOp extends BatchOperator <EasyTransferConfigTrainBatchOp>
	implements EasyTransferConfigTrainParams <EasyTransferConfigTrainBatchOp> {

	private static final List <String> resPyFiles = Arrays.asList(
		"res:///tf_algos/easytransfer/run_easytransfer_train_main.py"
	);

	// `mainScriptFileName` and `entryFuncName` are the main file and its entrypoint provided by our wrapping
	private static final String mainScriptFileName = "res:///tf_algos/easytransfer/run_easytransfer_train_main.py";

	private final ResourcePluginFactory factory;

	public EasyTransferConfigTrainBatchOp() {
		this(new Params());
	}

	public EasyTransferConfigTrainBatchOp(Params params) {
		super(params);
		factory = new ResourcePluginFactory();
	}

	@Override
	public EasyTransferConfigTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];

		Params params = getParams();

		if (null != getSelectedCols()) {
			in = in.select(getSelectedCols());
		}

		String pythonEnv = getPythonEnv();
		if (StringUtils.isNullOrWhitespaceOnly(pythonEnv)) {
			pythonEnv = DLEnvConfig.getTF115DefaultPythonEnv(factory);
		}

		Map <String, String> userParams = JsonConverter.fromJson(getUserParams(),
			new TypeToken <Map <String, String>>() {}.getType());
		userParams.put("config_json", getConfigJson());

		ExternalFilesConfig externalFilesConfig = params.contains(HasUserFiles.USER_FILES)
			? ExternalFilesConfig.fromJson(params.get(HasUserFiles.USER_FILES))
			: new ExternalFilesConfig();
		externalFilesConfig.addFilePaths(resPyFiles);

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp()
			.setUserFiles(externalFilesConfig)
			.setMainScriptFile(mainScriptFileName)
			.setNumWorkers(getNumWorkers())
			.setNumPSs(getNumPSs())
			.setUserParams(JsonConverter.toJson(userParams))
			.setPythonEnv(pythonEnv)
			.setIntraOpParallelism(getIntraOpParallelism())
			.setMLEnvironmentId(getMLEnvironmentId());

		BatchOperator <?>[] tfInputs;
		tfInputs = new BatchOperator <?>[inputs.length];
		tfInputs[0] = in;
		System.arraycopy(inputs, 1, tfInputs, 1, inputs.length - 1);
		BatchOperator <?> tfModel = tfTableModelTrainBatchOp.linkFrom(tfInputs);
		this.setOutputTable(tfModel.getOutputTable());
		return this;
	}
}
