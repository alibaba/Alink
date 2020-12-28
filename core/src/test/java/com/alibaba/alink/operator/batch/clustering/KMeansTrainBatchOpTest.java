package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.shared.clustering.HasKMeansDistanceType.DistanceType;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class KMeansTrainBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("0.1 0.2 0.3"),
			Row.of("0.2 0.4 0.6"),
			Row.of("0.3 0.6 0.9"),
			Row.of("9 9 9"),
			Row.of("9.1 9.1 9.1"),
			Row.of("9.2 9.2 9.2")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"vec"});

		KMeansTrainBatchOp op = new KMeansTrainBatchOp()
			.setK(2)
			.setVectorCol("vec")
			.setDistanceType(DistanceType.COSINE.name())
			.linkFrom(source);

		KMeansPredictBatchOp predictBatchOp = new KMeansPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(op, source);

		Assert.assertEquals(predictBatchOp.select("pred").distinct().count(), 2);
	}

	@Test
	public void testSparse() throws Exception {
		int len = 200, size = 10;
		Row[] array3 = new Row[len];
		for (int i = 0; i < array3.length; i++) {
			array3[i] = Row.of(i, new SparseVector(size, new int[] {i % 10, i * 2 % 10}, new double[] {1.0, 1.0}));
		}

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new TableSchema(new String[] {"id", "labelVector"}, new TypeInformation[] {
			Types.INT, VectorTypes.SPARSE_VECTOR
		}));

		KMeans kMeans = new KMeans()
			.setVectorCol("labelVector")
			.setK(5)
			.setPredictionCol("pred")
			.enableLazyPrintModelInfo();

		kMeans.fit(data).transform(data);

		BatchOperator.execute();
	}

}