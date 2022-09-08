package com.alibaba.alink.operator.local;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LocalOperatorTest {
	private static final Row[] data = new Row[] {
		Row.of(1L, 1L, 0.6),
		Row.of(2L, 2L, 0.8),
		Row.of(2L, 3L, 0.6),
		Row.of(3L, 1L, 0.6),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.4),
	};

	@Test
	public void testLazy() {
		LocalOperator <?> source
			= new TableSourceLocalOp(new MTable(Arrays.asList(data), new String[] {"u", "i", "r"}));
		source.lazyPrint(3, "title");
		source.lazyPrint(3, "title2");
		source.lazyCollect(System.out::println);
		List <Row> results = source.collect();
		System.out.println(results);
	}
}
