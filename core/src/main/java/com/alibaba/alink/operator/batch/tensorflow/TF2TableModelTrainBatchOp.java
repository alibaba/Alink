package com.alibaba.alink.operator.batch.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.dl.BaseDLTableModelTrainBatchOp;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.params.tensorflow.TFTableModelTrainParams;

import java.util.Collections;

/**
 * Run TF2 scripts to train a model.
 * <p>
 * The model must be saved in SavedModel format and be exported in a given directory. The directory (named using
 * timestamps) is zipped and returned to Alink side as a two-column Alink Model.
 */
@NameCn("TF2表模型训练")
public class TF2TableModelTrainBatchOp
	extends BaseDLTableModelTrainBatchOp <TF2TableModelTrainBatchOp> implements
	TFTableModelTrainParams <TF2TableModelTrainBatchOp> {

	public TF2TableModelTrainBatchOp() {
		this(new Params());
	}

	public TF2TableModelTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	protected void initDLSystemParams() {
		resPyFiles = Collections.singletonList("res:///entries/tf2_train_entry.py");
		mainScriptFileName = "res:///entries/tf2_train_entry.py";
		numPss = getNumPSs();
		userMainScriptRename = "tf_user_main.py";
		version = Version.TF231;
	}
}
