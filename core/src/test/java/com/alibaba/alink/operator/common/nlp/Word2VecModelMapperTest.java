package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.params.nlp.HasPredMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Word2VecModelMapperTest extends AlinkTestBase {

	@Test
	public void predictResult() {

		String type = "avg";

		assertEquals(
			HasPredMethod.PredMethod.valueOf(type.trim().toUpperCase()),
			HasPredMethod.PredMethod.AVG);
	}
}