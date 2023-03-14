package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.io.reader.FileSplitReader;
import com.alibaba.alink.operator.common.io.reader.QuoteUtil;

import java.io.IOException;

public class CsvInputFormatBeta extends GenericCsvInputFormatBeta <CsvFileInputSplit> {

	public CsvInputFormatBeta(FileSplitReader reader,
							  String lineDelim, boolean ignoreFirstLine) {
		super(reader, lineDelim, ignoreFirstLine);
	}

	public CsvInputFormatBeta(FileSplitReader reader,
							  String lineDelim, boolean ignoreFirstLine,
							  Character quoteChar) {
		super(reader, lineDelim, ignoreFirstLine, quoteChar);
	}

	public CsvInputFormatBeta(FileSplitReader reader,
							  String lineDelim, boolean ignoreFirstLine,
							  boolean unsplittable, Character quoteChar) {
		super(reader, lineDelim, ignoreFirstLine, unsplittable, quoteChar);
	}

	@Override
	public void open(CsvFileInputSplit split) throws IOException {
		super.open(split);
		if (split.start > 0 || ignoreFirstLine) {
			if (!readLine()) {
				// if the first partial record already pushes the stream over
				// the limit of our split, then no record starts within this split
				setEnd(true);
			}
		} else {
			fillBuffer(0);
		}
	}

	public void openWithoutSkipLine(CsvFileInputSplit split) throws IOException {
		super.open(split);
	}
	
	@Override
	public CsvFileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		// if parsing quote character, file is unsplittable
		if (this.unsplittable) {
			minNumSplits = 1;
		}

		CsvFileInputSplit[] splits;
		splits = new CsvFileInputSplit[minNumSplits];
		long contentLength = reader.getFileLength();
		for (int i = 0; i < splits.length; i++) {
			splits[i] = new CsvFileInputSplit(minNumSplits, i, contentLength);
		}
		return splits;
	}

	/**
	 * This format is serialize a split to row of string.
	 */
	public static class CsvSplitInputFormat extends CsvInputFormatBeta {
		private static final int BUFFER_SIZE = 1024 * 1024;

		public CsvSplitInputFormat(FileSplitReader reader, String lineDelim, boolean ignoreFirstLine) {
			super(reader, lineDelim, ignoreFirstLine);
		}

		public CsvSplitInputFormat(FileSplitReader reader, String lineDelim, boolean ignoreFirstLine,
								   Character quoteChar) {
			super(reader, lineDelim, ignoreFirstLine, false, quoteChar);
		}

		@Override
		public void open(CsvFileInputSplit split) throws IOException {
			super.openWithoutSkipLine(split);
		}

		/**
		 * This format is used to scan each split. It only returns one record then set end flag to true.
		 * The record includes quote character num and a string that stores variable used to rebuild the split.
		 *
		 * @param record Object that may be reused.
		 * @return quote count, split status and split serialized result
		 * @throws IOException
		 */
		@Override
		public Row nextRecord(Row record) throws IOException {
			long quoteNum = QuoteUtil.analyzeSplit(this.reader, quoteCharacter);

			this.setEnd(true);
			this.reader.close();

			return Row.of(quoteNum, this.reader.getSplitNumber(), this.currentSplit.toString());
		}
	}
}
