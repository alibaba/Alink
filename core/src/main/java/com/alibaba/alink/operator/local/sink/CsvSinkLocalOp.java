package com.alibaba.alink.operator.local.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.MTableUtil.FlatMapFunction;
import com.alibaba.alink.common.MTableUtil.GenericFlatMapFunction;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.io.filesystem.copy.csv.TextOutputFormat;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FlattenCsvFromRow;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FormatCsvFunc;
import com.alibaba.alink.operator.common.io.partition.CsvSinkCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.LocalUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.CsvSinkBatchParams;

import org.apache.commons.lang3.SerializationUtils;

import java.util.List;

@NameCn("CSV文件导出")
public final class CsvSinkLocalOp extends BaseSinkLocalOp <CsvSinkLocalOp>
	implements CsvSinkBatchParams <CsvSinkLocalOp> {

	public CsvSinkLocalOp() {
		this(new Params());
	}

	public CsvSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	protected CsvSinkLocalOp sinkFrom(LocalOperator <?> in) {
		if (getPartitionCols() != null) {

			LocalUtils.partitionAndWriteFile(
				in,
				new CsvSinkCollectorCreator(
					new FormatCsvFunc(in.getColTypes(), getFieldDelimiter(), getQuoteChar()),
					new FlattenCsvFromRow(getRowDelimiter()),
					getRowDelimiter()
				),
				getParams()
			);

		} else {

			final String filePath = getFilePath().getPathStr();
			final String fieldDelim = getFieldDelimiter();
			final TypeInformation <?>[] types = in.getColTypes();
			final Character quoteChar = getQuoteChar();

			FileSystem.WriteMode mode = FileSystem.WriteMode.NO_OVERWRITE;
			if (getOverwriteSink()) {
				mode = FileSystem.WriteMode.OVERWRITE;
			}

			final MTable input = in.getOutputTable();

			final FormatCsvFunc formatCsvFunc = new FormatCsvFunc(types, fieldDelim, quoteChar);

			final byte[] serialize = SerializationUtils.serialize(formatCsvFunc);

			List <Row> rows = MTableUtil.flatMapWithMultiThreads(input.getRows(), getParams(), new FlatMapFunction() {

				private FormatCsvFunc localFormatCsvFunc = null;

				@Override
				public void flatMap(Row row, Collector <Row> collector) throws Exception {

					if (localFormatCsvFunc == null) {
						localFormatCsvFunc = SerializationUtils.deserialize(serialize);
						localFormatCsvFunc.open(null);
					}

					collector.collect(localFormatCsvFunc.map(row));
				}
			});

			final FlattenCsvFromRow flattenCsvFromRow = new FlattenCsvFromRow(getRowDelimiter());

			List <String> flatten = MTableUtil.flatMapWithMultiThreads(rows, getParams(),
				new GenericFlatMapFunction <Row, String>() {
					@Override
					public void flatMap(Row input, Collector <String> collector) throws Exception {
						collector.collect(flattenCsvFromRow.map(input));
					}
				});

			TextOutputFormat <String> tof = new TextOutputFormat <>(
				new Path(filePath), getFilePath().getFileSystem(), getRowDelimiter()
			);

			tof.setWriteMode(mode);

			output(flatten, tof, getNumFiles());
		}

		return this;
	}
}

