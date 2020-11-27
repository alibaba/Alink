package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.DocWordSplitCount;
import com.alibaba.alink.params.nlp.DocWordCountParams;

/**
 * calculate doc word count.
 */
public final class DocWordCountBatchOp extends BatchOperator <DocWordCountBatchOp>
	implements DocWordCountParams <DocWordCountBatchOp> {
	private static final long serialVersionUID = 4163509124304798730L;

	public DocWordCountBatchOp() {
		this(null);
	}

	public DocWordCountBatchOp(Params parameters) {
		super(parameters);
	}

	@Override
	public DocWordCountBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		this.setOutputTable(in
			.udtf(this.getContentCol(), new String[] {"word", "cnt"},
				new DocWordSplitCount(this.getWordDelimiter()), new String[] {this.getDocIdCol()})
			.getOutputTable());

		return this;
	}
}
