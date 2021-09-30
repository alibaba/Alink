package com.alibaba.alink.operator.batch.tensorflow;


import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.tensorflow.TensorFlow2BatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

public class TensorFlow2BatchOpTest {

	@Category(DLTest.class)
	@Test
	public void testPs() throws Exception {
		BatchOperator.setParallelism(3);
		BatchOperator <?> source = new RandomTableSourceBatchOp()
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

		TensorFlow2BatchOp tensorFlow2BatchOp = new TensorFlow2BatchOp()
			.setUserFiles(new String[] {"res:///tf_dnn_batch.py"})
			.setMainScriptFile("res:///tf_dnn_batch.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(2)
			.setNumPSs(1)
			.setOutputSchemaStr("model_id long, model_info string")
			.setPythonEnv(DLTestConstants.LOCAL_TF231_ENV)
			.linkFrom(source);
		tensorFlow2BatchOp.print();
	}

	@Category(DLTest.class)
	@Test
	public void testAllReduce() throws Exception {
		BatchOperator.setParallelism(3);
		BatchOperator <?> source = new RandomTableSourceBatchOp()
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

		TensorFlow2BatchOp tensorFlow2BatchOp = new TensorFlow2BatchOp()
			.setUserFiles(new String[] {"res:///tf_dnn_batch.py"})
			.setMainScriptFile("res:///tf_dnn_batch.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setNumWorkers(3)
			.setNumPSs(0)
			.setOutputSchemaStr("model_id long, model_info string")
			.setPythonEnv(DLTestConstants.LOCAL_TF231_ENV)
			.linkFrom(source);
		tensorFlow2BatchOp.print();
	}

	@Category(DLTest.class)
	@Test
	public void testWithAutoWorkersPSs() throws Exception {
		BatchOperator.setParallelism(3);
		BatchOperator <?> source = new RandomTableSourceBatchOp()
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

		TensorFlow2BatchOp tensorFlow2BatchOp = new TensorFlow2BatchOp()
			.setUserFiles(new String[] {"res:///tf_dnn_batch.py"})
			.setMainScriptFile("res:///tf_dnn_batch.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.setOutputSchemaStr("model_id long, model_info string")
			.setPythonEnv(DLTestConstants.LOCAL_TF231_ENV)
			.linkFrom(source);
		tensorFlow2BatchOp.print();
	}
}
