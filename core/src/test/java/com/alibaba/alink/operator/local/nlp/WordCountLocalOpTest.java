package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class WordCountLocalOpTest extends TestCase {
	@Test
	public void test() throws Exception {
		Row[] array2 = new Row[] {
			Row.of("doc0", "中国 的 文化"),
			Row.of("doc1", "只要 功夫 深"),
			Row.of("doc2", "北京 的 拆迁"),
			Row.of("doc3", "人名 的 名义")
		};
		MemSourceLocalOp data = new MemSourceLocalOp(array2, new String[] {"docid", "content"});
		WordCountLocalOp wordCountLocalOp = new WordCountLocalOp()
			.setSelectedCol("content")
			.setWordDelimiter(" ");
		Assert.assertEquals(wordCountLocalOp.linkFrom(data).collectStatistics().count(), 10);
	}
}