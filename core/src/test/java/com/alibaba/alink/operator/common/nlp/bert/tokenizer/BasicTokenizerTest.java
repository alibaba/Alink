package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class BasicTokenizerTest extends AlinkTestBase {

	@Test
	public void tokenize() {
	}

	@Test
	public void cleanText() {
	}

	@Test
	public void tokenizeChineseChars() {
		Assert.assertEquals(
			" 你  好 ，world！",
			BertTokenizerImpl.BasicTokenizer.tokenizeChineseChars("你好，world！")
		);
		System.out.println(Character.getType('，'));
		System.out.println("你好，world！".codePoints().count());
	}

	@Test
	public void isChineseChar() {
		Assert.assertTrue(BertTokenizerImpl.BasicTokenizer.isChineseChar('你'));
		Assert.assertFalse(BertTokenizerImpl.BasicTokenizer.isChineseChar('a'));
	}
}
