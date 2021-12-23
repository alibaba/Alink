package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.nlp.Word2VecPredictBatchOp;
import com.alibaba.alink.operator.batch.nlp.Word2VecTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for Word2Vec
 */

public class Word2VecTest extends AlinkTestBase {

	@Test
	public void train() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"docid", "content"},
			new TypeInformation <?>[] {Types.LONG(), Types.STRING()}
		);
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of(0L, "老王 是 我们 团队 里 最胖 的"));
		rows.add(Row.of(1L, "老黄 是 第二 胖 的"));
		rows.add(Row.of(2L, "胖"));
		rows.add(Row.of(3L, "胖 胖 胖"));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		Word2Vec word2Vec = new Word2Vec()
			.setSelectedCol("content")
			.setOutputCol("output")
			.setMinCount(1);

		List <Row> result = word2Vec.fit(source).transform(source).collect();

		Assert.assertEquals(rows.size(), result.size());
	}

	@Test
	public void trainBatchOp() throws Exception {
		TableSchema schema = new TableSchema(
			new String[] {"docid", "content"},
			new TypeInformation <?>[] {Types.LONG(), Types.STRING()}
		);
		List <Row> rows = new ArrayList <>();
		rows.add(Row.of(0L, "老王 是 我们 团队 里 最胖 的"));
		rows.add(Row.of(1L, "老黄 是 第二 胖 的"));
		rows.add(Row.of(2L, "胖"));
		rows.add(Row.of(3L, "胖 胖 胖"));

		MemSourceBatchOp source = new MemSourceBatchOp(rows, schema);

		Word2VecTrainBatchOp word2VecTrainBatchOp = new Word2VecTrainBatchOp()
			.setSelectedCol("content")
			.setMinCount(1)
			.linkFrom(source)
			.lazyCollectTrainInfo(trainInfo -> {
				Assert.assertEquals(1, trainInfo.getNumIter());
				Assert.assertEquals(Long.valueOf(10L), trainInfo.getNumVocab());
			});

		Word2VecPredictBatchOp word2VecPredictBatchOp = new Word2VecPredictBatchOp()
			.setSelectedCol("content")
			.setOutputCol("output");

		Assert.assertEquals(
			rows.size(),
			word2VecPredictBatchOp
				.linkFrom(word2VecTrainBatchOp, source)
				.collect()
				.size()
		);
	}
}