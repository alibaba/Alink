package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.csv.TextOutputFormat;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FlattenCsvFromRow;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FormatCsvFunc;

import java.io.IOException;

public class CsvSinkCollectorCreator implements SinkCollectorCreator {
	private final FormatCsvFunc formatCsvFunc;
	private final FlattenCsvFromRow flattenCsvFromRow;

	private final String rowDelimiter;

	public CsvSinkCollectorCreator(FormatCsvFunc formatCsvFunc, FlattenCsvFromRow flattenCsvFromRow,
								   String rowDelimiter) {
		this.formatCsvFunc = formatCsvFunc;
		this.flattenCsvFromRow = flattenCsvFromRow;
		this.rowDelimiter = rowDelimiter;
	}

	@Override
	public Collector <Row> createCollector(FilePath filePath) throws IOException {
		TextOutputFormat <String> textOutputFormat = new TextOutputFormat <>(
			filePath.getPath(), filePath.getFileSystem(), rowDelimiter
		);

		textOutputFormat.open(0, 1);

		return new Collector <Row>() {
			@Override
			public void collect(Row record) {
				try {
					textOutputFormat.writeRecord(
						flattenCsvFromRow.map(formatCsvFunc.map(record))
					);
				} catch (Exception e) {
					throw new AkUnclassifiedErrorException("CsvSinkCollectorCreator collect error. ",e);
				}
			}

			@Override
			public void close() {
				try {
					textOutputFormat.close();
				} catch (IOException e) {
					throw new AkUnclassifiedErrorException("CsvSinkCollectorCreator close error. ",e);
				}
			}
		};
	}
}
