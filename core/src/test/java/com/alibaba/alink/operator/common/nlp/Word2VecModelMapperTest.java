package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.params.nlp.HasPredMethod;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Word2VecModelMapperTest {

	@Test
	public void predictResult() {

		String type = "avg";

		assertEquals(
			HasPredMethod.PredMethod.valueOf(type.trim().toUpperCase()),
			HasPredMethod.PredMethod.AVG);
	}
}