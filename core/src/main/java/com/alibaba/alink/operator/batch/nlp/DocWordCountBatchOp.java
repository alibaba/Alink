package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.DocWordSplitCount;
import com.alibaba.alink.params.nlp.DocWordCountParams;

/**
 * calculate doc word count.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "docIdCol")
@ParamSelectColumnSpec(name = "contentCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本词频统计")
@NameEn("Document Word Count")
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
