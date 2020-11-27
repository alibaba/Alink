package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.params.nlp.KeywordsExtractionParams;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Deal with the document within the timeInterval.
 */
public class KeywordsExtractionMap implements MapFunction <Row, Row> {
	private static final long serialVersionUID = -4466936915090248543L;
	private Params params;
	private int index;
	private OutputColsHelper outputColsHelper;

	public KeywordsExtractionMap(Params params, int index, OutputColsHelper outputColsHelper) {
		this.params = params;
		this.index = index;
		this.outputColsHelper = outputColsHelper;
	}

	@Override
	public Row map(Row row) throws Exception {
		Integer topN = this.params.get(KeywordsExtractionParams.TOP_N);

		Row in = new Row(2);
		in.setField(0, 1);
		in.setField(1, row.getField(index));
		Row[] rows = TextRank.getKeyWords(in,
			params.get(KeywordsExtractionParams.DAMPING_FACTOR),
			params.get(KeywordsExtractionParams.WINDOW_SIZE),
			params.get(KeywordsExtractionParams.MAX_ITER),
			params.get(KeywordsExtractionParams.EPSILON));
		Arrays.sort(rows, new Comparator <Row>() {
			@Override
			public int compare(Row row1, Row row2) {
				Double v1 = (double) row1.getField(2);
				Double v2 = (double) row2.getField(2);
				return v2.compareTo(v1);
			}
		});
		int len = Math.min(rows.length, topN);
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < len; i++) {
			builder.append(rows[i].getField(1));
			if (i != len - 1) {
				builder.append(" ");
			}
		}
		return outputColsHelper.getResultRow(row, Row.of(builder.toString()));
	}
}
