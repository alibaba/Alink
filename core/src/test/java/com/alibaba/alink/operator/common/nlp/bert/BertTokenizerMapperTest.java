package com.alibaba.alink.operator.common.nlp.bert;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.operator.common.nlp.bert.BertTokenizerMapper;
import com.alibaba.alink.params.tensorflow.bert.HasBertModelName;
import com.alibaba.alink.params.tensorflow.bert.HasMaxSeqLengthDefaultAsNull;
import com.alibaba.alink.params.tensorflow.bert.HasTextCol;
import com.alibaba.alink.params.tensorflow.bert.HasTextPairCol;
import org.junit.Assert;
import org.junit.Test;

public class BertTokenizerMapperTest {
	@Test
	public void test() throws Exception {
		Params params = new Params();
		params.set(HasBertModelName.BERT_MODEL_NAME, "bert-base-uncased");
		params.set(HasTextCol.TEXT_COL, "text_a");
		params.set(HasTextPairCol.TEXT_PAIR_COL, "text_b");

		TableSchema dataSchema = TableSchema.builder()
			.field("text_a", Types.STRING)
			.field("text_b", Types.STRING)
			.build();

		BertTokenizerMapper mapper = new BertTokenizerMapper(dataSchema, params);
		mapper.open();
		Row result = mapper.map(Row.of("sequence builders", "multi-sequence build"));
		System.out.println(result);
		mapper.close();

		Assert.assertEquals(new IntTensor(new int[] {101, 5537, 16472, 102, 4800, 1011, 5537, 3857, 102}),
			result.getField(2));
		Assert.assertEquals(new IntTensor(new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1}),
			result.getField(3));
		Assert.assertEquals(new IntTensor(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1}),
			result.getField(4));
	}

	@Test
	public void testMaxLengthPadding() throws Exception {
		Params params = new Params();
		params.set(HasBertModelName.BERT_MODEL_NAME, "bert-base-uncased");
		params.set(HasTextCol.TEXT_COL, "text_a");
		params.set(HasTextPairCol.TEXT_PAIR_COL, "text_b");
		params.set(HasMaxSeqLengthDefaultAsNull.MAX_SEQ_LENGTH, 16);

		TableSchema dataSchema = TableSchema.builder()
			.field("text_a", Types.STRING)
			.field("text_b", Types.STRING)
			.build();

		BertTokenizerMapper mapper = new BertTokenizerMapper(dataSchema, params);
		mapper.open();
		Row result = mapper.map(Row.of("sequence builders", "multi-sequence build"));
		System.out.println(result);
		mapper.close();

		Assert.assertEquals(new IntTensor(new int[] {101, 5537, 16472, 102, 4800, 1011, 5537, 3857, 102, 0, 0, 0, 0, 0, 0, 0}),
			result.getField(2));
		Assert.assertEquals(new IntTensor(new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0}),
			result.getField(3));
		Assert.assertEquals(new IntTensor(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0}),
			result.getField(4));
	}
}
