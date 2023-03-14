package com.alibaba.alink.operator.common.io.partition;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.FileProcFunction;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.common.io.csv.GenericCsvInputFormatBeta;
import com.alibaba.alink.operator.common.io.reader.FSFileSplitReader;

import java.io.IOException;

public class CsvSourceCollectorCreator implements SourceCollectorCreator {

	private final String rowDelim;

	private final Character quoteChar;

	private final boolean ignoreFirstLine;

	private final String[] dataFieldNames;

	private final TypeInformation[] dataFieldTypes;

	public CsvSourceCollectorCreator(TableSchema dummySchema, String rowDelim, boolean ignoreFirstLine,
									 Character quoteChar) {
		this.dataFieldNames = dummySchema.getFieldNames();
		this.dataFieldTypes = dummySchema.getFieldTypes();
		this.rowDelim = rowDelim;
		this.ignoreFirstLine = ignoreFirstLine;
		this.quoteChar = quoteChar;
	}

	@Override
	public TableSchema schema() {
		return new TableSchema(dataFieldNames, dataFieldTypes);
	}

	@Override
	public void collect(FilePath filePath, Collector <Row> collector) throws IOException {
		AkUtils.getFromFolderForEach(
			filePath,
			new FileProcFunction <FilePath, Boolean>() {
				@Override
				public Boolean apply(FilePath filePath) throws IOException {
					FSFileSplitReader reader = new FSFileSplitReader(filePath);
					GenericCsvInputFormatBeta inputFormat = reader.getInputFormat(rowDelim, ignoreFirstLine,
						quoteChar);

					try {
						inputFormat.open(new FileInputSplit(1, filePath.getPath(), 0, reader.getFileLength(), null));

						while (!inputFormat.reachedEnd()) {
							Row record = inputFormat.nextRecord(null);
							if (record != null) {
								collector.collect(record);
							} else {
								break;
							}
						}

					} finally {
						inputFormat.close();
					}
					return true;
				}
			});
	}
}
