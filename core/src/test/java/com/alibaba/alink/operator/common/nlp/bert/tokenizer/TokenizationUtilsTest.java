package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils;
import org.junit.Assert;
import org.junit.Test;

public class TokenizationUtilsTest {

	@Test
	public void isControl() {
		Assert.assertFalse(TokenizationUtils.isControl(' '));
		Assert.assertFalse(TokenizationUtils.isControl('\t'));
		Assert.assertFalse(TokenizationUtils.isControl('\r'));
		Assert.assertFalse(TokenizationUtils.isControl('\n'));
		Assert.assertTrue(TokenizationUtils.isControl('\u007f'));
	}

	@Test
	public void isWhitespace() {
		Assert.assertTrue(TokenizationUtils.isWhitespace(' '));
		Assert.assertTrue(TokenizationUtils.isWhitespace('\t'));
		Assert.assertTrue(TokenizationUtils.isWhitespace('\r'));
		Assert.assertTrue(TokenizationUtils.isWhitespace('\n'));
		Assert.assertFalse(TokenizationUtils.isWhitespace('\u007f'));
	}
}
