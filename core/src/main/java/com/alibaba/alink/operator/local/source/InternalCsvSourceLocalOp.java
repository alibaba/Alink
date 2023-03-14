package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.MTableUtil.FlatMapFunction;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.io.filesystem.copy.csv.RowCsvInputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.csv.GenericCsvInputFormat;
import com.alibaba.alink.operator.common.io.partition.CsvSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.LocalUtils;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.params.io.CsvSourceParams;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Internal
final class InternalCsvSourceLocalOp extends BaseSourceLocalOp <InternalCsvSourceLocalOp>
	implements CsvSourceParams <InternalCsvSourceLocalOp> {

	public InternalCsvSourceLocalOp() {
		this(new Params());
	}

	public InternalCsvSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		final String filePath = getFilePath().getPathStr();
		final String schemaStr = getSchemaStr();
		final String fieldDelim = getFieldDelimiter();
		final String rowDelim = getRowDelimiter();
		final Character quoteChar = getQuoteChar();
		final boolean skipBlankLine = getSkipBlankLine();
		final boolean lenient = getLenient();

		final String[] colNames = TableUtil.getColNames(schemaStr);
		final TypeInformation <?>[] colTypes = TableUtil.getColTypes(schemaStr);
		final TableSchema schema = new TableSchema(colNames, colTypes);

		boolean ignoreFirstLine = getIgnoreFirstLine();
		String protocol = "";

		try {
			URL url = new URL(filePath);
			protocol = url.getProtocol();
		} catch (MalformedURLException ignored) {
		}

		List <Row> rows;
		TableSchema dummySchema = new TableSchema(new String[] {"f1"}, new TypeInformation[] {Types.STRING});

		String partitions = getPartitions();

		if (partitions == null) {

			if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
				HttpFileSplitReader reader = new HttpFileSplitReader(filePath);
				rows = createInput(
					new GenericCsvInputFormat(reader, dummySchema.getFieldTypes(), rowDelim, rowDelim,
						ignoreFirstLine),
					new RowTypeInfo(dummySchema.getFieldTypes(), dummySchema.getFieldNames()), getParams());
			} else {
				RowCsvInputFormat inputFormat = new RowCsvInputFormat(
					new Path(filePath), dummySchema.getFieldTypes(),
					rowDelim, rowDelim, new int[] {0}, true,
					getFilePath().getFileSystem()
				);
				inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
				rows = createInput(inputFormat, getParams());
			}

		} else {

			Tuple2 <List <Row>, TableSchema> schemaAndData;

			try {
				schemaAndData = LocalUtils.readFromPartitionLocal(getParams(),
					new CsvSourceCollectorCreator(dummySchema, rowDelim, ignoreFirstLine, quoteChar)
				);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

			rows = schemaAndData.f0;

		}

		////todo error in multi-threads
		//RowCollector rowCollector = new RowCollector();
		//CsvUtil.ParseCsvFunc func = new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine, lenient);
		//try {
		//	func.open(null);
		//	for (Row row : rows) {
		//		func.flatMap(row, rowCollector);
		//	}
		//} catch (Exception ex) {
		//	throw new RuntimeException("Error in ParseCsvFunc open.");
		//}
		//return new MTable(rowCollector.getRows(), schema);

		final CsvUtil.ParseCsvFunc func
			= new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine, lenient);

		final byte[] serialized = SerializationUtils.serialize(func);

		rows = MTableUtil.flatMapWithMultiThreads(new MTable(rows, schema), getParams(),
			new FlatMapFunction() {
				private transient CsvUtil.ParseCsvFunc func = null;

				@Override
				public void flatMap(Row row, Collector <Row> collector) throws Exception {

					if (func == null) {
						func = SerializationUtils.deserialize(serialized);
						func.open(null);
					}

					func.flatMap(row, collector);
				}
			});
		return new MTable(rows, schema);
	}

}
