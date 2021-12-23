package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.dl.BertResources;
import com.alibaba.alink.common.dl.BertResources.ModelName;
import com.alibaba.alink.common.dl.BertResources.ResourceType;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.testutil.categories.DLTest;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class BertTextEmbeddingBatchOpTest {

    @Category(DLTest.class)
    @Test
    public void linkFrom() throws Exception {
        AlinkGlobalConfiguration.setPrintProcessInfo(true);
        PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

        RegisterKey registerKey = BertResources.getRegisterKey(ModelName.BASE_CHINESE, ResourceType.SAVED_MODEL);
        pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

        Row[] rows1 = new Row[]{
            Row.of(1L, "An english sentence."),
            Row.of(2L, "这是一个中文句子"),
        };

        for (int i = 0; i < 2; i += 1) {
            rows1 = ArrayUtils.addAll(rows1, rows1);
        }
        System.out.println("length = " + rows1.length);

        BatchOperator.setParallelism(2);
        BatchOperator<?> data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[]{"sentence_id", "sentence_text"}));

        BertTextEmbeddingBatchOp bertEmb = new BertTextEmbeddingBatchOp()
            .setSelectedCol("sentence_text").setOutputCol("embedding").setLayer(-2)
            .setDoLowerCase(true)
            .setIntraOpParallelism(4);
        data.link(bertEmb).lazyPrint(10);
        BatchOperator.execute();
    }

    @Test
    public void testLongSentence() throws Exception {
        AlinkGlobalConfiguration.setPrintProcessInfo(true);
        PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

        RegisterKey registerKey = BertResources.getRegisterKey(ModelName.BASE_CHINESE, ResourceType.SAVED_MODEL);
        pluginDownloader.downloadPlugin(registerKey.getName(), registerKey.getVersion());

        Row[] rows1 = new Row[]{
            Row.of(1L, "An english sentence."),
            Row.of(2L, "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"
                + "这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子这是一个中文句子"),
        };

        for (int i = 0; i < 2; i += 1) {
            rows1 = ArrayUtils.addAll(rows1, rows1);
        }
        System.out.println("length = " + rows1.length);

        BatchOperator.setParallelism(2);
        BatchOperator<?> data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[]{"sentence_id", "sentence_text"}));

        BertTextEmbeddingBatchOp bertEmb = new BertTextEmbeddingBatchOp()
            .setSelectedCol("sentence_text").setOutputCol("embedding").setLayer(-2)
            .setDoLowerCase(true)
            .setIntraOpParallelism(4);
        data.link(bertEmb).lazyPrint(10);
        BatchOperator.execute();
    }
}
