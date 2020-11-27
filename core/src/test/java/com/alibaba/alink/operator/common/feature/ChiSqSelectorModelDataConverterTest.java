package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class ChiSqSelectorModelDataConverterTest {

	@Test
	public void test() {
		VectorChiSqSelectorModelDataConverter converter = new VectorChiSqSelectorModelDataConverter();

		int[] modelData = new int[] {3, 5, 4};
		List <Row> rowList = new ArrayList <>();
		ListCollector <Row> rows = new ListCollector(rowList);
		converter.save(modelData, rows);
		int[] indices = converter.load(rowList);
		assertArrayEquals(modelData, indices);

	}

}