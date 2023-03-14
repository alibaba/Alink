package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.csv.RowCsvInputFormat;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.io.partition.CsvSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.params.io.CsvSourceParams;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Internal
@IoOpAnnotation(name = "internal_csv", ioType = IOType.SourceBatch)
public final class InternalCsvSourceBatchOp extends BaseSourceBatchOp <InternalCsvSourceBatchOp>
	implements CsvSourceParams <InternalCsvSourceBatchOp> {

	private static final long serialVersionUID = -3105428980027751387L;

	public InternalCsvSourceBatchOp() {
		this(new Params());
	}

	public InternalCsvSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(InternalCsvSourceBatchOp.class), params);
	}

	public InternalCsvSourceBatchOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, schemaStr)
		);
	}

	public InternalCsvSourceBatchOp(String filePath, TableSchema schema) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(schema))
		);
	}

	public InternalCsvSourceBatchOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes,
									String fieldDelim, String rowDelim) {
		this(new Params()
			.set(FILE_PATH, new FilePath(filePath).serialize())
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
			.set(FIELD_DELIMITER, fieldDelim)
			.set(ROW_DELIMITER, rowDelim)
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

		final String[] colNames = TableUtil.getColNames(schemaStr);
		final TypeInformation <?>[] colTypes = TableUtil.getColTypes(schemaStr);

		boolean ignoreFirstLine = getIgnoreFirstLine();
		String protocol = "";

		try {
			URL url = new URL(filePath);
			protocol = url.getProtocol();
		} catch (MalformedURLException ignored) {
		}

		DataSet <Row> rows;
		ExecutionEnvironment execEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();
		TableSchema dummySchema = new TableSchema(new String[] {"f1"}, new TypeInformation[] {Types.STRING});

		String partitions = getPartitions();

		if (partitions == null) {

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

		} else {

			Tuple2 <DataSet <Row>, TableSchema> schemaAndData;

			try {
				schemaAndData = DataSetUtil.readFromPartitionBatch(
					getParams(), getMLEnvironmentId(),
					new CsvSourceCollectorCreator(dummySchema, rowDelim, ignoreFirstLine, quoteChar)
				);
			} catch (IOException e) {
				throw new AkUnclassifiedErrorException(
					String.format("Fail to list directories in %s and select partitions", getFilePath().getPathStr()));
			}

			rows = schemaAndData.f0;

		}

		rows = rows.flatMap(new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine, lenient));

		return DataSetConversionUtil.toTable(getMLEnvironmentId(), rows, colNames, colTypes);
	}
}
