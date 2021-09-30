package com.alibaba.alink.operator.stream.tensorflow;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TFTableModelPredictStreamOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

public class TFTableModelPredictStreamOpTest {

	@Category(DLTest.class)
	@Test
	public void test() throws Exception {
		BatchOperator.setParallelism(3);

		BatchOperator <?> source = new RandomTableSourceBatchOp()
			.setNumRows(100L)
			.setNumCols(10);

		String[] colNames = source.getColNames();
		source = source.select("*, case when RAND() > 0.5 then 1. else 0. end as label");
		String label = "label";

		StreamOperator<?> streamSource = new RandomTableSourceStreamOp()
			.setNumCols(10)
			.setMaxRows(100L);

		Map <String, Object> userParams = new HashMap <>();
		userParams.put("featureCols", JsonConverter.toJson(colNames));
		userParams.put("labelCol", label);
		userParams.put("batch_size", 16);
		userParams.put("num_epochs", 1);

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp()
			.setUserFiles(new String[] {"res:///tf_dnn_train.py"})
			.setMainScriptFile("res:///tf_dnn_train.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(2)
			.setNumPSs(1)
			.setPythonEnv(DLTestConstants.LOCAL_TF115_ENV)
			.linkFrom(source);

		TFTableModelPredictStreamOp tfTableModelPredictStreamOp = new TFTableModelPredictStreamOp(tfTableModelTrainBatchOp)
			.setOutputSchemaStr("logits double")
			.setOutputSignatureDefs(new String[] {"logits"})
			.setSignatureDefKey("predict")
			.setSelectedCols(colNames)
			.linkFrom(streamSource);
		tfTableModelPredictStreamOp.print();
		StreamOperator.execute();
	}
}
