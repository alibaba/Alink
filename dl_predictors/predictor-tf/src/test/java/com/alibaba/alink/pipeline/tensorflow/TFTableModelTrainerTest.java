package com.alibaba.alink.pipeline.tensorflow;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

public class TFTableModelTrainerTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(3);

		BatchOperator<?> source = new RandomTableSourceBatchOp()
			.setNumRows(100L)
			.setNumCols(10);

		String[] colNames = source.getColNames();
		source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
		String label = "label";

		Map <String, Object> userParams = new HashMap <>();
		userParams.put("featureCols", JsonConverter.toJson(colNames));
		userParams.put("labelCol", label);
		userParams.put("batch_size", 16);
		userParams.put("num_epochs", 1);

		TFTableModelTrainer trainer = new TFTableModelTrainer()
			.setUserFiles(new String[] {"res:///tf_dnn_train.py"})
			.setMainScriptFile("res:///tf_dnn_train.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(2)
			.setNumPSs(1)
			.setOutputSchemaStr("logits double")
			.setOutputSignatureDefs(new String[]{"logits"})
			.setSignatureDefKey("predict")
			.setInferSelectedCols(colNames);

		TFTableModelPredictor model = trainer.fit(source);
		model.transform(source).print();
		BatchOperator.setParallelism(savedParallelism);
	}
}
