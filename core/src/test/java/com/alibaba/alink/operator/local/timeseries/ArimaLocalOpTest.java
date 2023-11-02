//package com.alibaba.alink.operator.local.timeseries;
//
//import org.apache.flink.types.Row;
//
//import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
//import com.alibaba.alink.operator.local.sql.GroupByLocalOp;
//import junit.framework.TestCase;
//import org.junit.Test;
//
//import java.sql.Timestamp;
//import java.util.Arrays;
//import java.util.List;
//
//public class ArimaLocalOpTest extends TestCase {
//
//	@Test
//	public void test() throws Exception {
//		List <Row> mTableData = Arrays.asList(
//			Row.of(1, new Timestamp(1), 10.0),
//			Row.of(1, new Timestamp(2), 11.0),
//			Row.of(1, new Timestamp(3), 12.0),
//			Row.of(1, new Timestamp(4), 13.0),
//			Row.of(1, new Timestamp(5), 14.0),
//			Row.of(1, new Timestamp(6), 15.0),
//			Row.of(1, new Timestamp(7), 16.0),
//			Row.of(1, new Timestamp(8), 17.0),
//			Row.of(1, new Timestamp(9), 18.0),
//			Row.of(1, new Timestamp(10), 19.0)
//		);
//
//		MemSourceLocalOp source = new MemSourceLocalOp(mTableData, new String[] {"id", "ts", "val"});
//
//		source.link(
//			new GroupByLocalOp()
//				.setGroupByPredicate("id")
//				.setSelectClause("mtable_agg(ts, val) as data")
//		).link(new ArimaLocalOp()
//			.setValueCol("data")
//			.setOrder(1, 2, 1)
//			.setPredictNum(12)
//			.setPredictionCol("predict")
//		).print();
//	}
//
//}