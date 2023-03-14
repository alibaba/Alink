package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelInfoBatchOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Test
 */
public class DbscanBatchOpTest extends AlinkTestBase {
	private static Row[] array3 = new Row[] {
		Row.of("id_1", "2.0,3.0"),
		Row.of("id_2", "2.1,3.1"),
		Row.of("id_3", "200.1,300.1"),
		Row.of("id_4", "200.2,300.2"),
		Row.of("id_5", "200.3,300.3"),
		Row.of("id_6", "200.4,300.4"),
		Row.of("id_7", "200.5,300.5"),
		Row.of("id_8", "200.6,300.6"),
		Row.of("id_9", "2.1,3.1"),
		Row.of("id_10", "2.1,3.1"),
		Row.of("id_11", "2.1,3.1"),
		Row.of("id_12", "2.1,3.1"),
		Row.of("id_13", "2.3,3.2"),
		Row.of("id_14", "2.3,3.2"),
		Row.of("id_15", "2.8,3.2"),
		Row.of("id_16", "300.,3.2"),
		Row.of("id_17", "2.2,3.2"),
		Row.of("id_18", "2.4,3.2"),
		Row.of("id_19", "2.5,3.2"),
		Row.of("id_20", "2.5,3.2"),
		Row.of("id_21", "2.1,3.1")
	};

	@Test
	public void testSparse() throws Exception {
		Row[] array3 = new Row[200];
		for (int i = 0; i < array3.length; i++) {
			array3[i] = Row.of(String.valueOf(i), rand(100, 5));
		}

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"id", "labelVector"});
		MemSourceStreamOp dataS = new MemSourceStreamOp(Arrays.asList(array3), new String[] {"id", "labelVector"});

		DbscanBatchOp op = new DbscanBatchOp()
			.setMinPoints(10)
			.setEpsilon(1.3)
			.setMinPoints(4)
			.setPredictionCol("pred")
			.setIdCol("id")
			.setVectorCol("labelVector")
			.linkFrom(data);

		DbscanPredictBatchOp predict = new DbscanPredictBatchOp()
			.setPredictionCol("pred")
			.linkFrom(op.getSideOutput(0), data);

		predict.collect();
	}

	@Test
	public void testLazyPrint() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"id", "labelVector"});
		DbscanBatchOp op = new DbscanBatchOp()
			.setMinPoints(5)
			.setEpsilon(0.5)
			.setPredictionCol("pred")
			.setIdCol("id")
			.setVectorCol("labelVector")
			.linkFrom(data);

		op.lazyCollectModelInfo(new Consumer <DbscanModelInfoBatchOp.DbscanModelInfo>() {
			@Override
			public void accept(DbscanModelInfoBatchOp.DbscanModelInfo dbscanModelInfo) {
				for (int i = 0; i < dbscanModelInfo.getClusterNumber(); i++) {
					System.out.println("ClusterId: " + i);
					System.out.println("Core Points:" + Arrays.toString(dbscanModelInfo.getCorePoints(i)));
					System.out.println("Linked Points:" + Arrays.toString(dbscanModelInfo.getLinkedPoints(i)));
				}
			}
		});
		op.lazyPrintModelInfo();

		BatchOperator.execute();

	}

	public static SparseVector rand(int n, int numValue) {
		Random random = new Random();
		int[] index = new int[numValue];
		double[] values = new double[numValue];
		for (int i = 0; i < numValue; i++) {
			index[i] = random.nextInt(n);
			values[i] = random.nextDouble();
		}
		return new SparseVector(n, index, values);
	}

	@Test
	public void linkFrom() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"id", "labelVector"});

		DbscanBatchOp trainBatchOp = new DbscanBatchOp()
			.setPredictionCol("pred")
			.setMinPoints(4)
			.setEpsilon(1.0)
			.setIdCol("id")
			.setVectorCol("labelVector")
			.linkFrom(data);

		String cluster1 = Arrays.asList(new String[] {"id_3", "id_4", "id_5", "id_6", "id_7", "id_8"}).toString();
		String cluster2 = Arrays.asList(
			new String[] {"id_1", "id_10", "id_11", "id_12", "id_13", "id_14", "id_15", "id_17", "id_18", "id_19",
				"id_2", "id_20", "id_21", "id_9"}).toString();
		String cluster3 = Arrays.asList(new String[] {"id_16"}).toString();

		List <String> expect = Arrays.asList(cluster1, cluster2, cluster3);
		List <String> res = extractClusters(trainBatchOp).collect();

		Assert.assertEquals(res.size(), 3);
		Assert.assertTrue(res.containsAll(expect));
	}

	@Test
	public void testCosine() throws Exception {
		Row[] array3 = new Row[] {
			Row.of("id_1", "1,1"),
			Row.of("id_2", "1,2"),
			Row.of("id_3", "2,1"),
			Row.of("id_4", "2,2"),
			Row.of("id_5", "2,3"),
			Row.of("id_6", "3,3"),
			Row.of("id_7", "3,4"),
			Row.of("id_8", "4,3"),
			Row.of("id_9", "1,3"),
			Row.of("id_10", "1,4"),
			Row.of("id_11", "3,2"),
			Row.of("id_12", "4,2"),
			Row.of("id_13", "2,4"),
			Row.of("id_14", "3,1"),
			Row.of("id_15", "5,5"),
			Row.of("id_16", "5,6"),
			Row.of("id_17", "6,5"),
			Row.of("id_18", "7,5"),
			Row.of("id_19", "7,6"),
			Row.of("id_20", "7,7"),
			Row.of("id_21", "7,8"),
			Row.of("id_22", "6,7")
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"id", "labelVector"});

		DbscanBatchOp trainBatchOp = new DbscanBatchOp()
			.setPredictionCol("pred")
			.setMinPoints(7)
			.setEpsilon(0.01)
			.setDistanceType("COSINE")
			.setIdCol("id")
			.setVectorCol("labelVector")
			.linkFrom(data);

		Assert.assertEquals(trainBatchOp.getSideOutput(0).count(), 13);
	}

	private static DataSet <String> extractClusters(BatchOperator dbscanRes) {
		return dbscanRes
			.select(new String[] {"id", "pred"})
			.getDataSet()
			.groupBy(1)
			.reduceGroup(new GroupReduceFunction <Row, String>() {
				private static final long serialVersionUID = -2223776597492862180L;

				@Override
				public void reduce(Iterable <Row> values, Collector <String> out) throws Exception {
					List <String> cluster = new ArrayList <>();
					values.forEach(v -> cluster.add(v.getField(0).toString()));
					Collections.sort(cluster);
					out.collect(cluster.toString());
				}
			});
	}

	@Test
	public void testDense() throws Exception {
		Row[] array3 = new Row[] {
			Row.of("id_1", "1,1"),
			Row.of("id_2", "1,2"),
			Row.of("id_3", "2,1"),
			Row.of("id_4", "2,2"),
			Row.of("id_5", "2,3"),
			Row.of("id_6", "3,3"),
			Row.of("id_7", "3,4"),
			Row.of("id_8", "4,3"),
			Row.of("id_9", "1,3"),
			Row.of("id_10", "1,4"),
			Row.of("id_11", "3,2"),
			Row.of("id_12", "4,2"),
			Row.of("id_13", "2,4"),
			Row.of("id_14", "3,1"),
			Row.of("id_15", "5,5"),
			Row.of("id_16", "5,6"),
			Row.of("id_17", "6,5"),
			Row.of("id_18", "7,5"),
			Row.of("id_19", "7,6"),
			Row.of("id_20", "7,7"),
			Row.of("id_21", "7,8"),
			Row.of("id_22", "6,7")

		};
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"id", "labelVector"});

		DbscanBatchOp trainBatchOp = new DbscanBatchOp()
			.setMinPoints(4)
			.setEpsilon(1.2)
			.setPredictionCol("pred")
			.setIdCol("id")
			.setVectorCol("labelVector")
			.linkFrom(data);

		HashMap <String, Tuple2 <String, Long>> map = new HashMap <>();
		map.put("id_5", Tuple2.of("CORE", 0L));
		map.put("id_6", Tuple2.of("CORE", 0L));
		map.put("id_7", Tuple2.of("LINKED", 0L));
		map.put("id_8", Tuple2.of("LINKED", 0L));
		map.put("id_9", Tuple2.of("CORE", 0L));
		map.put("id_10", Tuple2.of("LINKED", 0L));
		map.put("id_11", Tuple2.of("CORE", 0L));
		map.put("id_12", Tuple2.of("LINKED", 0L));
		map.put("id_13", Tuple2.of("CORE", 0L));
		map.put("id_14", Tuple2.of("LINKED", 0L));
		map.put("id_15", Tuple2.of("NOISE", -2147483648L));
		map.put("id_16", Tuple2.of("NOISE", -2147483648L));
		map.put("id_17", Tuple2.of("NOISE", -2147483648L));
		map.put("id_18", Tuple2.of("NOISE", -2147483648L));
		map.put("id_19", Tuple2.of("LINKED", 1L));
		map.put("id_20", Tuple2.of("CORE", 1L));
		map.put("id_21", Tuple2.of("LINKED", 1L));
		map.put("id_22", Tuple2.of("LINKED", 1L));
		map.put("id_1", Tuple2.of("LINKED", 0L));
		map.put("id_2", Tuple2.of("CORE", 0L));
		map.put("id_3", Tuple2.of("CORE", 0L));
		map.put("id_4", Tuple2.of("CORE", 0L));

		String cluster1 = Arrays.asList(new String[] {"id_19", "id_20", "id_21", "id_22"}).toString();
		String cluster2 = Arrays.asList(
			new String[] {"id_1", "id_10", "id_11", "id_12", "id_13", "id_14", "id_2", "id_3", "id_4", "id_5", "id_6",
				"id_7", "id_8", "id_9"}).toString();
		String cluster3 = Arrays.asList(new String[] {"id_15", "id_16", "id_17", "id_18"}).toString();

		List <String> expect = Arrays.asList(cluster1, cluster2, cluster3);
		List <String> res = extractClusters(trainBatchOp).collect();

		Assert.assertEquals(res.size(), 3);
		Assert.assertTrue(res.containsAll(expect));

	}

}