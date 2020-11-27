package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class WordCountBatchOpTest extends AlinkTestBase {

	@Test
	public void linkFrom() throws Exception {
		Row[] array2 = new Row[] {
			Row.of("doc0", "中国 的 文化"),
			Row.of("doc1", "只要 功夫 深"),
			Row.of("doc2", "北京 的 拆迁"),
			Row.of("doc3", "人名 的 名义")
		};
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array2), new String[] {"docid", "content"});
		WordCountBatchOp wordCountBatchOp = new WordCountBatchOp()
			.setSelectedCol("content")
			.setWordDelimiter(" ");
		Assert.assertEquals(wordCountBatchOp.linkFrom(data).count(), 10);
	}
}