package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.DLTestConstants;
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.BertResources;
import com.alibaba.alink.common.dl.BertResources.ModelName;
import com.alibaba.alink.common.dl.BertResources.ResourceType;
import com.alibaba.alink.common.dl.DLEnvConfig;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class BertTextClassifierTest {
	@Test
	public void test() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		RegisterKey registerKey = DLEnvConfig.getRegisterKey(Version.TF115);
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		registerKey = TFPredictorClassLoaderFactory.getRegisterKey();
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		registerKey = BertResources.getRegisterKey(ModelName.BASE_CHINESE, ResourceType.CKPT);
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		int savedParallelism = MLEnvironmentFactory.getDefault().getExecutionEnvironment().getParallelism();
		BatchOperator.setParallelism(1);
		String url = DLTestConstants.CHN_SENTI_CORP_HTL_PATH;

		String schemaStr = "label bigint, review string";
		BatchOperator <?> data = new CsvSourceBatchOp()
			.setFilePath(url)
			.setSchemaStr(schemaStr)
			.setIgnoreFirstLine(true);
		data = data.where("review is not null");
		data = new ShuffleBatchOp().linkFrom(data);

		BertTextClassifier classifier = new BertTextClassifier()
			.setTextCol("review")
			.setLabelCol("label")
			.setNumEpochs(0.01)
			.setNumFineTunedLayers(1)
			.setMaxSeqLength(128)
			.setBertModelName("Base-Chinese")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");
		BertClassificationModel model = classifier.fit(data);
		BatchOperator <?> predict = model.transform(data.firstN(300));
		predict.print();
		BatchOperator.setParallelism(savedParallelism);
	}
}
