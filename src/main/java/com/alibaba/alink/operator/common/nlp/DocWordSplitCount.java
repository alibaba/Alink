package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Split the document and return word count.
 */
public class DocWordSplitCount extends TableFunction <Row> {

	private String delimiter;

	public DocWordSplitCount(String delimiter) {
		this.delimiter = delimiter;
	}

	public void eval(String content) {
		if (null == content || content.length() == 0) {
			return;
		}
		String[] words = content.split(this.delimiter);

		HashMap <String, Long> map = new HashMap <>(0);

		for (String word : words) {
			if (word.length() > 0) {
				map.merge(word, 1L, Long::sum);
			}
		}

		for (Map.Entry <String, Long> entry : map.entrySet()) {
			collect(Row.of(entry.getKey(), entry.getValue()));
		}
	}

	@Override
	public TypeInformation <Row> getResultType() {
		return new RowTypeInfo(Types.STRING, Types.LONG);
	}
}
