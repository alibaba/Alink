package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.HugeMultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link MultiStringIndexer}.
 */

public class MultiStringIndexerTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static Row[] rows = new Row[] {
		Row.of("a", 1L),
		Row.of(null, 1L),
		Row.of("b", 1L),
		Row.of("b", 3L),

	};

	private Map <String, Long> map1;
	private Map <Long, Long> map2;

	@Before
	public void setup() {
		map1 = new HashMap <>();
		map1.put("a", 1L);
		map1.put("b", 0L);
		map1.put(null, null);

		map2 = new HashMap <>();
		map2.put(1L, 0L);
		map2.put(3L, 1L);
	}

	@Test
	public void testMultiStringIndexer() throws Exception {
		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1"});

		MultiStringIndexer stringIndexer = new MultiStringIndexer()
			.setSelectedCols("f0", "f1")
			.setOutputCols("f0_index", "f1_index")
			.setHandleInvalid("skip")
			.setStringOrderType("frequency_desc");

		data = stringIndexer.fit(data).transform(data);
		Assert.assertEquals(data.getColNames().length, 4);
		List <Row> result = data.collect();
		Assert.assertEquals(result.size(), 4);

		result.forEach(row -> {
			String token1 = (String) row.getField(0);
			Long token2 = (Long) row.getField(1);
			Assert.assertEquals(map1.get(token1), row.getField(2));
			Assert.assertEquals(map2.get(token2), row.getField(3));
		});

		StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rows), new String[] {"f0", "f1"});
		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(stringIndexer.fit(data).transform(streamData));
		StreamOperator.execute();
		result = collectSinkStreamOp.getAndRemoveValues();
		Assert.assertEquals(result.size(), 4);

		result.forEach(row -> {
			String token1 = (String) row.getField(0);
			Long token2 = (Long) row.getField(1);
			Assert.assertEquals(map1.get(token1), row.getField(2));
			Assert.assertEquals(map2.get(token2), row.getField(3));
		});
	}

	@Test
	public void testHugeMultiStringIndexerPredictBatchOp() {
		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1"});

		MultiStringIndexerTrainBatchOp stringIndexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f1", "f0")
			.setStringOrderType("frequency_desc");

		HugeMultiStringIndexerPredictBatchOp predictor = new HugeMultiStringIndexerPredictBatchOp()
			.setSelectedCols("f0")
			.setReservedCols("f0")
			.setOutputCols("f0_index")
			.setHandleInvalid("skip");

		stringIndexer.linkFrom(data);
		data = predictor.linkFrom(stringIndexer, data);

		Assert.assertEquals(data.getColNames().length, 2);
		List <Row> result = data.collect();
		Assert.assertEquals(result.size(), 4);

		result.forEach(row -> {
			String token = (String) row.getField(0);
			Assert.assertEquals(map1.get(token), row.getField(1));
		});
	}

	@Test
	public void testMultiStringIndexerPredictBatchOp() {
		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1"});

		MultiStringIndexerTrainBatchOp stringIndexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f1", "f0")
			.setStringOrderType("frequency_desc");

		MultiStringIndexerPredictBatchOp predictor = new MultiStringIndexerPredictBatchOp()
			.setSelectedCols("f0")
			.setReservedCols("f0")
			.setOutputCols("f0_index")
			.setHandleInvalid("skip");

		stringIndexer.linkFrom(data);
		data = predictor.linkFrom(stringIndexer, data);

		Assert.assertEquals(data.getColNames().length, 2);
		List <Row> result = data.collect();
		Assert.assertEquals(result.size(), 4);

		result.forEach(row -> {
			String token = (String) row.getField(0);
			Assert.assertEquals(map1.get(token), row.getField(1));
		});
	}

	@Test
	public void testException() throws Exception {
		thrown.expect(RuntimeException.class);
		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"f0", "f1"});

		MultiStringIndexerTrainBatchOp stringIndexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0")
			.setStringOrderType("frequency_desc")
			.linkFrom(data.firstN(3));

		HugeMultiStringIndexerPredictBatchOp predictor = new HugeMultiStringIndexerPredictBatchOp()
			.setSelectedCols("f0", "f1")
			.setReservedCols("f0")
			.setOutputCols("f0_index")
			.setHandleInvalid("skip")
			.linkFrom(stringIndexer, data);

		predictor.print();

		predictor.setSelectedCols("f0").setHandleInvalid("keep").linkFrom(stringIndexer, data).collect();
	}
}