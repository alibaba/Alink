package com.alibaba.alink.pipeline.nlp;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for DocCountVectorizer.
 */
public class DocCountVectorizerTest {
	private Row[] rows = new Row[] {
			Row.of(0, "That is an English book", 1),
			Row.of(1, "Have a good day", 1)
	};
	private Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});
	private Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence", "label"});
	@Test
	public void testDefault() throws Exception {
		DocCountVectorizer op = new DocCountVectorizer()
			.setSelectedCol("sentence")
			.setOutputCol("features")
			.setFeatureType("TF");

		PipelineModel model = new Pipeline().add(op).fit(data);

		Table res = model.transform(data);

		List<SparseVector> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("features"), SparseVector.class).collect();

		Assert.assertEquals(list.size(), 2);
		Assert.assertEquals(list.get(0).getValues().length, 5);
		Assert.assertEquals(list.get(1).getValues().length, 4);
		for(int i = 0; i < list.get(0).getValues().length; i++){
			Assert.assertEquals(list.get(0).getValues()[i], 0.2, 0.1);
		}
		for(int i = 0; i < list.get(1).getValues().length; i++){
			Assert.assertEquals(list.get(1).getValues()[i], 0.25, 0.1);
		}
		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
