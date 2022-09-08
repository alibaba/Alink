package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class TfidfLocalOpTest extends TestCase {
	@Test
	public void test() throws Exception {

		Row[] array2 = new Row[] {
			Row.of("doc0", "中国", 1L),
			Row.of("doc0", "的", 1L),
			Row.of("doc0", "文化", 1L),
			Row.of("doc1", "只要", 1L),
			Row.of("doc1", "中国", 2L),
			Row.of("doc1", "功夫", 1L),
			Row.of("doc1", "深", 1L),
			Row.of("doc2", "北京", 1L),
			Row.of("doc2", "的", 1L),
			Row.of("doc2", "拆迁", 1L),
			Row.of("doc3", "人名", 1L),
			Row.of("doc3", "的", 1L),
			Row.of("doc3", "名义", 1L)
		};

		TfidfLocalOp tfidf = new MemSourceLocalOp(array2, new String[] {"docid", "word", "cnt"})
			.link(new TfidfLocalOp()
				.setDocIdCol("docid")
				.setWordCol("word")
				.setCountCol("cnt")
			)
			.print();

		//Print tf idf result
		Assert.assertEquals(tfidf.collectStatistics().count(), 13);

	}

}