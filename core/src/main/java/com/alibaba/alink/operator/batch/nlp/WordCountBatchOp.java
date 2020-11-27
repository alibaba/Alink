package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.nlp.WordCountParams;

/**
 * Extract all words and their counts of occurrences from documents.
 * Words are recognized by using the delimiter.
 */
public final class WordCountBatchOp extends BatchOperator <WordCountBatchOp>
	implements WordCountParams <WordCountBatchOp> {

	private static final long serialVersionUID = -3874218655921411838L;

	public WordCountBatchOp() {
		super(null);
	}

	public WordCountBatchOp(Params params) {
		super(params);
	}

	@Override
	public WordCountBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String sentenceColName = this.getSelectedCol();
		String delim = this.getWordDelimiter();
		this.setOutputTable(WordCountUtil.splitDocAndCount(in, sentenceColName, delim).getOutputTable());
		return this;
	}
}
