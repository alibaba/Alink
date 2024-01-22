package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.nlp.WordCountParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.nlp.WordCountUtil.COUNT_COL_NAME;
import static com.alibaba.alink.operator.common.nlp.WordCountUtil.WORD_COL_NAME;

/**
 * Extract all words and their counts of occurrences from documents.
 * Words are recognized by using the delimiter.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("单词计数")
public final class WordCountLocalOp extends LocalOperator <WordCountLocalOp>
	implements WordCountParams <WordCountLocalOp> {

	public WordCountLocalOp() {
		super(null);
	}

	public WordCountLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String docColName = this.getSelectedCol();
		String wordDelimiter = this.getWordDelimiter();

		int index = TableUtil.findColIndexWithAssert(in.getSchema(), docColName);
		HashMap <String, Long> map = new HashMap <>();
		for (Row row : in.getOutputTable().getRows()) {
			String content = row.getField(index).toString();
			if (null == content || content.length() == 0) {
				continue;
			}
			for (String word : content.split(wordDelimiter)) {
				if (word.length() > 0) {
					map.merge(word, 1L, Long::sum);
				}
			}
		}

		ArrayList <Row> list = new ArrayList <>();
		for (Map.Entry <String, Long> entry : map.entrySet()) {
			list.add(Row.of(entry.getKey(), entry.getValue()));
		}

		this.setOutputTable(new MTable(list, WORD_COL_NAME + " string, " + COUNT_COL_NAME + " long"));
	}
}
