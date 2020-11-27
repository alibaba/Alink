package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;

import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StratifiedSampleWithSizeBatchOpTest {

	@Test
	public void testStratifiedSampleWithSize() throws Exception {
		Random r = new Random();
		int size = 50;
		List <Row> data = new ArrayList <>();
		for (int i = 0; i < size; i++) {
			data.add(Row.of("a", r.nextInt()));
		}
		MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[] {"key", "val"});
		StratifiedSampleWithSizeBatchOp sampleOp = new StratifiedSampleWithSizeBatchOp()
			.setStrataCol("key")
			.setStrataSizes("a:10")
			.setWithReplacement(true);

		long count = sourceOp.link(sampleOp).count();
		Assert.assertEquals(10, count);
	}

	@Test
	public void testStratifiedSampleWithSize1() throws Exception {
		Random r = new Random();
		int[] len = new int[] {200, 300, 400};
		String[] keys = new String[] {"a", "b", "c"};
		List <Row> data = new ArrayList <>();
		for (int i = 0; i < len.length; i++) {
			for (int j = 0; j < len[i]; j++) {
				data.add(Row.of(keys[i], r.nextInt(100)));
			}
		}
		MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[] {"key", "val"});
		StratifiedSampleWithSizeBatchOp sampleOp = new StratifiedSampleWithSizeBatchOp()
			.setStrataCol("key")
			.setStrataSizes("a:10,b:20,c:30")
			.setWithReplacement(true);

		Map <Object, Long> collect = sourceOp.link(sampleOp).
			collect().
			stream().
			map(e -> e.getField(0)).
			collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		Long a = collect.get(keys[0]);
		Long b = collect.get(keys[1]);
		Long c = collect.get(keys[2]);

		Assert.assertEquals(a.longValue(), 10L);
		Assert.assertEquals(b.longValue(), 20L);
		Assert.assertEquals(c.longValue(), 30L);
	}
}
