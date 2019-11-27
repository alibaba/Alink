package examples;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import org.junit.Test;

/**
 * Example for KMeans.
 */
public class KMeansExample {
	@Test
	public void main() throws Exception {
		Row[] rows = new Row[100];
		for (int i = 0; i < rows.length; i++) {
			rows[i] = Row.of(VectorUtil.toString(DenseVector.rand(10)));
		}

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"vector"});

		KMeans kMeans = new KMeans()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setK(2);

		Pipeline pipeline = new Pipeline().add(kMeans);

		Table res = pipeline.fit(data).transform(data);

		DataSetConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();
	}

}