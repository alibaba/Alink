package com.alibaba.alink.operator.common.nlp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Word2VecModelMapperTest {

	@Test
	public void predictResult() {

		String type = "avg";

		assertEquals(
			DocVecGenerator.InferVectorMethod.valueOf(type.trim().toUpperCase()),
			DocVecGenerator.InferVectorMethod.AVG);
	}
}