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

public class MultiStringIndexerTrainBatchOpTest extends AlinkTestBase {

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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		HashMap <String, Integer>[] mapList = new HashMap[2];
		mapList[0] = new HashMap <>();
		mapList[1] = new HashMap <>();
		model.lazyPrint(10);
		List <Row> list = model.collect();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			Long index = (Long) r.getField(0);
			if (index < 0L) {
				continue;
			}
			String token = (String) r.getField(1);
			Long id = (Long) r.getField(2);
			mapList[index.intValue()].put(token, id.intValue());
		}
		Assert.assertEquals(mapList[0].get("baseball").intValue(), 0);
		Assert.assertEquals(mapList[0].get("football").intValue(), 4);
		Assert.assertEquals(mapList[1].get("pair").intValue(), 0);
		Assert.assertEquals(mapList[1].get("apple").intValue(), 2);
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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("frequency_desc");
		BatchOperator model = stringindexer.linkFrom(data);
		HashMap <String, Integer>[] mapList = new HashMap[2];
		mapList[0] = new HashMap <>();
		mapList[1] = new HashMap <>();
		model.lazyPrint(10);
		List <Row> list = model.collect();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			Long index = (Long) r.getField(0);
			if (index < 0L) {
				continue;
			}
			String token = (String) r.getField(1);
			Long id = (Long) r.getField(2);
			mapList[index.intValue()].put(token, id.intValue());
		}
		Assert.assertEquals(mapList[0].get("baseball").intValue(), 4);
		Assert.assertEquals(mapList[0].get("football").intValue(), 0);
		Assert.assertEquals(mapList[1].get("pair").intValue(), 2);
		Assert.assertEquals(mapList[1].get("apple").intValue(), 0);
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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("random");
		BatchOperator model = stringindexer.linkFrom(data);
		model.print();
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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("alphabet_asc");
		BatchOperator model = stringindexer.linkFrom(data);
		HashMap <String, Integer>[] mapList = new HashMap[2];
		mapList[0] = new HashMap <>();
		mapList[1] = new HashMap <>();
		model.lazyPrint(10);
		List <Row> list = model.collect();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			Long index = (Long) r.getField(0);
			if (index < 0L) {
				continue;
			}
			String token = (String) r.getField(1);
			Long id = (Long) r.getField(2);
			mapList[index.intValue()].put(token, id.intValue());
		}
		Assert.assertEquals(mapList[0].get("baseball").intValue(), 0);
		Assert.assertEquals(mapList[0].get("tennis").intValue(), 4);
		Assert.assertEquals(mapList[1].get("pair").intValue(), 2);
		Assert.assertEquals(mapList[1].get("apple").intValue(), 0);
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
		BatchOperator <?> stringindexer = new MultiStringIndexerTrainBatchOp()
			.setSelectedCols("f0", "f1")
			.setStringOrderType("alphabet_desc");
		BatchOperator model = stringindexer.linkFrom(data);
		HashMap <String, Integer>[] mapList = new HashMap[2];
		mapList[0] = new HashMap <>();
		mapList[1] = new HashMap <>();
		model.lazyPrint(10);
		List <Row> list = model.collect();
		for (int i = 0; i < list.size(); i++) {
			Row r = list.get(i);
			Long index = (Long) r.getField(0);
			if (index < 0L) {
				continue;
			}
			String token = (String) r.getField(1);
			Long id = (Long) r.getField(2);
			mapList[index.intValue()].put(token, id.intValue());
		}
		Assert.assertEquals(mapList[0].get("baseball").intValue(), 4);
		Assert.assertEquals(mapList[0].get("tennis").intValue(), 0);
		Assert.assertEquals(mapList[1].get("pair").intValue(), 0);
		Assert.assertEquals(mapList[1].get("apple").intValue(), 2);
	}
}
