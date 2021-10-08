package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextClassifierTrainBatchOpTest {

	@Category(DLTest.class)
	public void testConfig(Integer parallelism, Integer numPSs, String checkpointFilePath) throws Exception {
		BatchOperator.setParallelism(parallelism);
		String url = DLTestConstants.CHN_SENTI_CORP_HTL_PATH;
		String schema = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schema)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");

		BertTextClassifierTrainBatchOp train = new BertTextClassifierTrainBatchOp()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(2.)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setNumPSs(numPSs)
			.setCheckpointFilePath(checkpointFilePath)
			.setModelPath(DLTestConstants.BERT_CHINESE_DIR)
			.setPythonEnv(DLTestConstants.LOCAL_TF115_ENV)
			.linkFrom(data);

		new AkSinkBatchOp()
			.setFilePath("/tmp/bert_text_classifier_model.ak")
			.setOverwriteSink(true)
			.linkFrom(train);
		BatchOperator.execute();
	}

	@Category(DLTest.class)
	@Test
	public void testSingleWorker() throws Exception {
		testConfig(1, null, null);
	}

	@Category(DLTest.class)
	@Test
	public void testSingleWorkerModelDir() throws Exception {
		testConfig(1, null, PythonFileUtils.createTempWorkDir("bert_text_classifier_train_"));
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersAllReduce() throws Exception {
		testConfig(3, 0, null);
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersAllReduceModelDir() throws Exception {
		testConfig(3, 0, PythonFileUtils.createTempWorkDir("bert_text_classifier_train_"));
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersPS() throws Exception {
		testConfig(3, null, null);
	}

	@Category(DLTest.class)
	@Test
	public void testMultiWorkersPSModelDir() throws Exception {
		testConfig(3, null, PythonFileUtils.createTempWorkDir("bert_text_classifier_train_"));
	}
}
