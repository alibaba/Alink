package com.alibaba.alink.pipeline.nlp;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
//import com.alibaba.alink.common.utils.RowTypeDataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for DocHashIDFVectorizer.
 */
public class DocHashCountVectorizerTest {
	@Test
	public void testIdf() throws Exception {
		Row[] rows = new Row[] {
			Row.of(0, "a b c d a a", 1),
			Row.of(1, "c c b a e", 1)
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});
		Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"id", "sentence", "label"});

		DocHashCountVectorizer op = new DocHashCountVectorizer()
			.setSelectedCol("sentence")
			.setNumFeatures(10)
			.setOutputCol("res");

		DocHashCountVectorizerModel model = op.fit(data);

		Table res = model.transform(data);

		List <SparseVector> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(res.select("res"), SparseVector.class).collect();

		Assert.assertArrayEquals(list.toArray(new SparseVector[0]),
			new SparseVector[] {new SparseVector(10, new int[]{3, 4, 5, 7}, new double[]{1.0, 3.0, 1.0, 1.0}),
				new SparseVector(10, new int[]{4, 5, 6, 7}, new double[]{1.0, 2.0, 1.0, 1.0})});

		res = model.transform(dataStream);

		DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID,res).print();

		MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
	}
}
