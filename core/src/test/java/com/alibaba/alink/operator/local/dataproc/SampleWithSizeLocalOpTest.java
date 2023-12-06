package com.alibaba.alink.operator.local.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SampleWithSizeLocalOpTest extends TestCase {

	@Test
	public void test1() {
		List <Row> rows = new ArrayList <>();
		for (int i = 0; i < 5; i++) {
			rows.add(Row.of(Integer.valueOf(i)));
		}

		LocalOperator <?> source = new MemSourceLocalOp(rows, "val int");

		source.print();

		source.link(new SampleWithSizeLocalOp().setSize(10).setWithReplacement(true).setRandomSeed(0)).print();

		source.link(new SampleWithSizeLocalOp().setSize(3).setWithReplacement(false).setRandomSeed(0)).print();
	}

}