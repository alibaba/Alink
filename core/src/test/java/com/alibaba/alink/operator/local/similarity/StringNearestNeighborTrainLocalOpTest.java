package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StringNearestNeighborTrainLocalOpTest extends TestCase {
	@Test
	public void testStringNearestNeighborTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "abcde", "aabce"),
			Row.of(1, "aacedw", "aabbed"),
			Row.of(2, "cdefa", "bbcefa"),
			Row.of(3, "bdefh", "ddeac"),
			Row.of(4, "acedm", "aeefbc")
		);
		LocalOperator <?> inOp = new MemSourceLocalOp(df, "id int, text1 string, text2 string");
		LocalOperator <?> train = new StringNearestNeighborTrainLocalOp().setIdCol("id").setSelectedCol("text1")
			.setMetric("LEVENSHTEIN_SIM").linkFrom(inOp);
		LocalOperator <?> predict = new StringNearestNeighborPredictLocalOp().setSelectedCol("text2").setTopN(3)
			.linkFrom(train, inOp);
		predict.print();
	}
}