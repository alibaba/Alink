package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for {@link IndexToString}.
 */

public class IndexToStringTest extends AlinkTestBase {
	private static Row[] rows = new Row[] {
		Row.of("0", "football"),
		Row.of("1", "football"),
		Row.of("2", "football"),
		Row.of("3", "basketball"),
		Row.of("4", "basketball"),
		Row.of("5", "tennis"),
	};

	@Test
	public void testIndexToString() throws Exception {
		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"id", "feature"});

		StringIndexer stringIndexer = new StringIndexer()
			.setModelName("string_indexer_model")
			.setSelectedCol("feature")
			.setOutputCol("feature_indexed")
			.setStringOrderType("frequency_asc");

		BatchOperator indexed = stringIndexer.fit(data).transform(data);

		IndexToString indexToString = new IndexToString()
			.setModelName("string_indexer_model")
			.setSelectedCol("feature_indexed")
			.setOutputCol("feature_indxed_unindexed");

		List <Row> unindexed = indexToString.transform(indexed).collect();
		unindexed.sort(new RowComparator(0));
		assertEquals(unindexed.get(0).getField(2), 2L);
		assertEquals(unindexed.get(3).getField(2), 1L);
		assertEquals(unindexed.get(5).getField(2), 0L);
		unindexed.forEach(row -> {
			Assert.assertEquals(row.getField(1), row.getField(3));
		});

		StreamOperator streamData = new MemSourceStreamOp(Arrays.asList(rows), new String[] {"id", "feature"});
		CollectSinkStreamOp collectSinkStreamOp = new CollectSinkStreamOp()
			.linkFrom(indexToString.transform(stringIndexer.fit(data).transform(streamData)));
		StreamOperator.execute();
		List <Row> result = collectSinkStreamOp.getAndRemoveValues();
		result.sort(new RowComparator(0));
		assertEquals(result.get(0).getField(2), 2L);
		assertEquals(result.get(3).getField(2), 1L);
		assertEquals(result.get(5).getField(2), 0L);
		result.forEach(row -> {
			Assert.assertEquals(row.getField(1), row.getField(3));
		});
	}

}