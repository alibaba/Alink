package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.csv.RowCsvInputFormat;

import java.io.IOException;

public class CsvSourceCollectorCreator implements SourceCollectorCreator {

	private final TableSchema dummySchema;

	private final String rowDelim;

	private final boolean ignoreFirstLine;

	public CsvSourceCollectorCreator(TableSchema dummySchema, String rowDelim, boolean ignoreFirstLine) {
		this.dummySchema = dummySchema;
		this.rowDelim = rowDelim;
		this.ignoreFirstLine = ignoreFirstLine;
	}

	@Override
	public TableSchema schema() {
		return dummySchema;
	}

	@Override
	public void collect(FilePath filePath, Collector <Row> collector) throws IOException {
		RowCsvInputFormat inputFormat = new RowCsvInputFormat(
			filePath.getPath(), dummySchema.getFieldTypes(),
			rowDelim, rowDelim, new int[] {0}, true,
			filePath.getFileSystem()
		);
		inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);

		try {
			inputFormat.open(new FileInputSplit(1, filePath.getPath(), 0, -1, null));

			while (!inputFormat.reachedEnd()) {
				collector.collect(inputFormat.nextRecord(null));
			}

		} finally {
			inputFormat.close();
		}

	}
}
