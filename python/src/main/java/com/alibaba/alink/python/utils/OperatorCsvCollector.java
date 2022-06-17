package com.alibaba.alink.python.utils;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.MTableSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvFormatter;

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class OperatorCsvCollector {

	/**
	 * Collect data from a list of {@link BatchOperator}s in CSV format.
	 *
	 * @param ops            a list of {@link BatchOperator}s.
	 * @param lineTerminator line terminator in CSV format.
	 * @param fieldDelimiter field terminator in CSV format.
	 * @param quoteChar      quote char in CSV format.
	 * @return Content of each {@link BatchOperator}.
	 */
	@SuppressWarnings("unused")
	public static List <String> collectToCsv(BatchOperator <?>[] ops, String lineTerminator, String fieldDelimiter,
											 Character quoteChar) throws Exception {
		List <LazyEvaluation <String>> lazyCsvs = lazyCollectToCsv(ops, lineTerminator, fieldDelimiter, quoteChar);
		BatchOperator.execute();
		List <String> csvs = new ArrayList <>();
		for (LazyEvaluation <String> lazyCsv : lazyCsvs) {
			csvs.add(lazyCsv.getLatestValue());
		}
		return csvs;
	}

	/**
	 * Lazy collect data from a list of {@link BatchOperator}s in CSV format.
	 *
	 * @param ops            a list of {@link BatchOperator}s.
	 * @param lineTerminator line terminator in CSV format.
	 * @param fieldDelimiter field terminator in CSV format.
	 * @param quoteChar      quote char in CSV format.
	 * @return {@link LazyEvaluation} of each {@link BatchOperator} for later callbacks.
	 */
	@SuppressWarnings("unused")
	public static List <LazyEvaluation <String>> lazyCollectToCsv(BatchOperator <?>[] ops, String lineTerminator,
																  String fieldDelimiter, Character quoteChar) {
		return Arrays.stream(ops).map(op -> {
			final LazyEvaluation <String> lazyCsv = new LazyEvaluation <>();
			BatchOperator <?> dop = op.link(new VectorSerializeBatchOp().setMLEnvironmentId(op.getMLEnvironmentId()))
				.link(new MTableSerializeBatchOp().setMLEnvironmentId(op.getMLEnvironmentId())).link(
					new TensorSerializeBatchOp().setMLEnvironmentId(op.getMLEnvironmentId()));
			dop.lazyCollect(rows -> {
				CsvFormatter csvFormatter = new CsvFormatter(dop.getColTypes(), fieldDelimiter, quoteChar);
				String ret = rows.stream().map(csvFormatter::format).collect(Collectors.joining(lineTerminator));
				lazyCsv.addValue(ret);
			});
			return lazyCsv;
		}).collect(Collectors.toList());
	}

	public static String mtableToCsv(MTable mTable, String lineTerminator, String fieldDelimiter,
									 Character quoteChar) throws Exception {
		try (StringWriter stringWriter = new StringWriter();
			 BufferedWriter bufferedWriter = new BufferedWriter(stringWriter)) {
			mTable.writeCsvToFile(bufferedWriter);
			bufferedWriter.flush();
			return stringWriter.toString();
		}
	}
}
