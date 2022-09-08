package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DocHashCountVectorizerTrainLocalOpTest extends TestCase {
	@Test
	public void test() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		LocalOperator <?> inOp1 = new MemSourceLocalOp(df, "id int, text string");
		LocalOperator <?> segment = new SegmentLocalOp().setSelectedCol("text").linkFrom(inOp1);
		LocalOperator <?> train = new DocHashCountVectorizerTrainLocalOp().setSelectedCol("text").linkFrom(segment);
		LocalOperator <?> predictBatch = new DocHashCountVectorizerPredictLocalOp().setSelectedCol("text").linkFrom(
			train, segment);
		train.print();
		predictBatch.print();
	}
}