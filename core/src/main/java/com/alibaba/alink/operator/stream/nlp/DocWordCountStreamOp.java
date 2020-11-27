package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.DocWordSplitCount;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.nlp.DocWordCountParams;

/**
 * calculate doc word count.
 */
public final class DocWordCountStreamOp extends StreamOperator <DocWordCountStreamOp>
	implements DocWordCountParams <DocWordCountStreamOp> {

	private static final long serialVersionUID = 8915973664798758920L;

	public DocWordCountStreamOp() {
		super(null);
	}

	public DocWordCountStreamOp(Params parameters) {
		super(parameters);
	}

	@Override
	public DocWordCountStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		this.setOutputTable(in
			.udtf(this.getContentCol(), new String[] {"word", "cnt"},
				new DocWordSplitCount(this.getWordDelimiter()), new String[] {this.getDocIdCol()})
			.getOutputTable());

		return this;
	}
}
