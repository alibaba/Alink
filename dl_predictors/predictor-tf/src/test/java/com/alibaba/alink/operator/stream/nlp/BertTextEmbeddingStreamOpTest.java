package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.BertResources;
import com.alibaba.alink.common.dl.BertResources.ModelName;
import com.alibaba.alink.common.dl.BertResources.ResourceType;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.junit.Test;

public class BertTextEmbeddingStreamOpTest {
	@Test
	public void linkFrom() throws Exception {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

		RegisterKey registerKey = BertResources.getRegisterKey(ModelName.BASE_CHINESE, ResourceType.SAVED_MODEL);
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		registerKey = TFPredictorClassLoaderFactory.getRegisterKey();
		pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

		Row[] rows1 = new Row[] {
			Row.of(1L, "An english sentence."),
			Row.of(2L, "这是一个中文句子"),
		};

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().setParallelism(1);

		StreamOperator <?> data = StreamOperator.fromTable(
			MLEnvironmentFactory.getDefault().createStreamTable(rows1, new String[] {"sentence_id", "sentence_text"}));

		BertTextEmbeddingStreamOp bertEmb = new BertTextEmbeddingStreamOp()
			.setSelectedCol("sentence_text").setOutputCol("embedding").setLayer(-2);
		data.link(bertEmb).print();

		StreamOperator.execute();
	}
}
