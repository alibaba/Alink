package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.WordCountUtil;
import com.alibaba.alink.params.nlp.WordCountParams;

/**
 * Extract all words and their counts of occurrences from documents.
 * Words are recognized by using the delimiter.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("单词计数")
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
