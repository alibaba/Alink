package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StringIndexerTrainBatchOpTest extends AlinkTestBase {
	@Test
	public void testFrequencyAsc() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		// baseball 1
		// basketball,pair,tennis,pingpang 2
		// footbal,banana 3
		// apple 5
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		List <Row> list = model.collect();
		HashMap <String, Integer> map = new HashMap <>();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			String token = (String) r.getField(0);
			Long id = (Long) r.getField(1);
			map.put(token, id.intValue());
		}
		Assert.assertEquals(map.get("baseball").intValue(), 0);
		Assert.assertEquals(map.get("apple").intValue(), 7);
		Assert.assertEquals(list.size(), 8);
	}

	@Test
	public void testFrequencyDesc() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		// baseball 1
		// basketball,pair,tennis,pingpang 2
		// footbal,banana 3
		// apple 5
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("frequency_desc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		List <Row> list = model.collect();
		HashMap <String, Integer> map = new HashMap <>();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			String token = (String) r.getField(0);
			Long id = (Long) r.getField(1);
			map.put(token, id.intValue());
		}
		Assert.assertEquals(map.get("baseball").intValue(), 7);
		Assert.assertEquals(map.get("apple").intValue(), 0);
		Assert.assertEquals(list.size(), 8);
	}

	@Test
	public void testRandom() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("random");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		List <Row> list = model.collect();
		Assert.assertEquals(list.size(), 8);
	}

	@Test
	public void testAlphabetAsc() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("alphabet_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		List <Row> list = model.collect();
		HashMap <String, Integer> map = new HashMap <>();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			String token = (String) r.getField(0);
			Long id = (Long) r.getField(1);
			map.put(token, id.intValue());
		}
		Assert.assertEquals(map.get("tennis").intValue(), 7);
		Assert.assertEquals(map.get("apple").intValue(), 0);
		Assert.assertEquals(list.size(), 8);
	}

	@Test
	public void testAlphabetDesc() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("football", "apple"),
			Row.of("basketball", "apple"),
			Row.of("basketball", "apple"),
			Row.of("tennis", "pair"),
			Row.of("tennis", "pair"),
			Row.of("pingpang", "banana"),
			Row.of("pingpang", "banana"),
			Row.of("baseball", "banana")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string,f1 string");
		BatchOperator <?> stringindexer = new StringIndexerTrainBatchOp()
			.setSelectedCol("f0")
			.setSelectedCols("f1")
			.setStringOrderType("alphabet_desc");
		BatchOperator model = stringindexer.linkFrom(data);
		model.lazyPrint(10);
		List <Row> list = model.collect();
		HashMap <String, Integer> map = new HashMap <>();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			String token = (String) r.getField(0);
			Long id = (Long) r.getField(1);
			map.put(token, id.intValue());
		}
		Assert.assertEquals(map.get("tennis").intValue(), 0);
		Assert.assertEquals(map.get("apple").intValue(), 7);
		Assert.assertEquals(list.size(), 8);
	}
}