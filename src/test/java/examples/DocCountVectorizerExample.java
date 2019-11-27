package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.nlp.DocCountVectorizer;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * Example for DocCountVectorizer.
 */
public class DocCountVectorizerExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[] {
			Row.of(new Object[] {0, "That is a English book", 1}),
			Row.of(new Object[] {1, "That Math book in on the desk", 1}),
			Row.of(new Object[] {2, "Do you like math", 1}),
			Row.of(new Object[] {3, "It is a good day today", 0}),
			Row.of(new Object[] {4, "Have a good day", 0})
		};
		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"id", "sentence", "label"});

		DocCountVectorizer op = new DocCountVectorizer()
			.setSelectedCol("sentence")
			.setOutputCol("features")
			.setFeatureType("TF_IDF");

		KMeans kMeans = new KMeans()
			.setVectorCol("features")
			.setPredictionCol("cluster_id")
			.setReservedCols(new String[] {"id", "label"})
			.setK(2);

		Pipeline pipeline = new Pipeline().add(op).add(kMeans);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, pipeline.fit(data).transform(data)).print();
	}
}
