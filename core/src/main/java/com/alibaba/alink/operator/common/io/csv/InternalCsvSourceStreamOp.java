package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.csv.RowCsvInputFormat;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.operator.stream.source.BaseSourceStreamOp;
import com.alibaba.alink.params.io.CsvSourceParams;

import java.net.MalformedURLException;
import java.net.URL;

@Internal
@IoOpAnnotation(name = "internal_csv", ioType = IOType.SourceStream)
public final class InternalCsvSourceStreamOp extends BaseSourceStreamOp <InternalCsvSourceStreamOp>
	implements CsvSourceParams <InternalCsvSourceStreamOp> {

	private static final long serialVersionUID = -1087793092068599835L;

	public InternalCsvSourceStreamOp() {
		this(new Params());
	}

	public InternalCsvSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(InternalCsvSourceStreamOp.class), params);
	}

	public InternalCsvSourceStreamOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, schemaStr)
		);
	}

	@Override
	public Table initializeDataSource() {
		final String filePath = getFilePath().getPathStr();
		final String schemaStr = getSchemaStr();
		final String fieldDelim = getFieldDelimiter();
		final String rowDelim = getRowDelimiter();
		final Character quoteChar = getQuoteChar();
		final boolean skipBlankLine = getSkipBlankLine();
		final boolean lenient = getLenient();

		final String[] colNames = CsvUtil.getColNames(schemaStr);
		final TypeInformation <?>[] colTypes = CsvUtil.getColTypes(schemaStr);

		boolean ignoreFirstLine = getIgnoreFirstLine();
		String protocol = "";

		try {
			URL url = new URL(filePath);
			protocol = url.getProtocol();
		} catch (MalformedURLException ignored) {
		}

		DataStream <Row> rows;
		StreamExecutionEnvironment execEnv =
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment();
		TableSchema dummySchema = new TableSchema(new String[] {"f1"}, new TypeInformation[] {Types.STRING});

		if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
			HttpFileSplitReader reader = new HttpFileSplitReader(filePath);
			rows = execEnv
				.createInput(
					new GenericCsvInputFormat(reader, dummySchema.getFieldTypes(), rowDelim, rowDelim,
						ignoreFirstLine),
					new RowTypeInfo(dummySchema.getFieldTypes(), dummySchema.getFieldNames()))
				.name("http_csv_source");
		} else {
			RowCsvInputFormat inputFormat = new RowCsvInputFormat(
				new Path(filePath), dummySchema.getFieldTypes(),
				rowDelim, rowDelim, new int[] {0}, true,
				getFilePath().getFileSystem()
			);
			inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
			rows = execEnv.createInput(inputFormat).name("csv_source");
		}

		rows = rows.flatMap(new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine, lenient));

		return DataStreamConversionUtil.toTable(getMLEnvironmentId(), rows, colNames, colTypes);
	}
}
