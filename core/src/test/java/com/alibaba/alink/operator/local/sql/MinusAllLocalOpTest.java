//package com.alibaba.alink.operator.local.sql;
//
//import com.alibaba.alink.operator.local.LocalOperator;
//import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
//import com.alibaba.alink.testutil.AlinkTestBase;
//import junit.framework.TestCase;
//import org.junit.Test;
//
//public class MinusAllLocalOpTest extends AlinkTestBase {
//	@Test
//	public void testMinusAllLocalOp() {
//		//String SCHEMA_STR
//		//	= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
//		//LocalOperator <?> data1 = new MemSourceLocalOp(IrisData.irisDoubleArray, SCHEMA_STR);
//		LocalOperator <?> data1 = new MemSourceLocalOp(IrisData.irisDoubleArray,
//			new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width", "category"});
//		data1.print();
//		LocalOperator <?> data2 = data1.filter("category='Iris-setosa'");
//		data2.print();
//		LocalOperator <?> minusAllOp = new MinusAllLocalOp();
//		minusAllOp.linkFrom(data1, data2).print();
//	}
//}