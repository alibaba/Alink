package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.testutil.categories.DLTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextClassifierTrainBatchOpTest {

	@Category(DLTest.class)
	public void testConfig(Integer parallelism, Integer numPSs, String checkpointFilePath) throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
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
			.linkFrom(data);
		Assert.assertTrue(train.count() > 1);
		BatchOperator.setParallelism(savedParallelism);
	}

	@Test
	public void testSingleWorker() throws Exception {
		testConfig(1, null, null);
	}

	@Test
	public void testSingleWorkerModelDir() throws Exception {
		testConfig(1, null, PythonFileUtils.createTempDir("bert_text_classifier_train_").toString());
	}

	@Test
	public void testMultiWorkersAllReduce() throws Exception {
		testConfig(3, 0, null);
	}

	@Test
	public void testMultiWorkersAllReduceModelDir() throws Exception {
		testConfig(3, 0, PythonFileUtils.createTempDir("bert_text_classifier_train_").toString());
	}

	@Test
	public void testMultiWorkersPS() throws Exception {
		testConfig(3, null, null);
	}

	@Test
	public void testMultiWorkersPSModelDir() throws Exception {
		testConfig(3, null, PythonFileUtils.createTempDir("bert_text_classifier_train_").toString());
	}

	@Test
	public void testDefaultMaxSeqLength() throws Exception {
		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);
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
			.setNumEpochs(1.)
			.setNumFineTunedLayers(1)
			.setBertModelName("Base-Chinese")
			.linkFrom(data);
		Assert.assertTrue(train.count() > 1);
		BatchOperator.setParallelism(savedParallelism);
	}
}
