package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.io.partition.CsvSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.reader.FSFileSplitReader;
import com.alibaba.alink.operator.common.io.reader.FileSplitReader;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.params.io.CsvSourceParams;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

@Internal
@IoOpAnnotation(name = "internal_csv_beta", ioType = IOType.SourceBatch)
public class InternalCsvSourceBetaBatchOp extends BaseSourceBatchOp <InternalCsvSourceBetaBatchOp>
	implements CsvSourceParams <InternalCsvSourceBetaBatchOp> {

	public InternalCsvSourceBetaBatchOp() {
		this(new Params());
	}

	public InternalCsvSourceBetaBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(InternalCsvSourceBetaBatchOp.class), params);

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
		DataSet <Row> splits;
		ExecutionEnvironment execEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();
		TableSchema dummySchema;

		String partitions = getPartitions();

		if (partitions == null) {
			FileSplitReader reader;

			if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
				reader = new HttpFileSplitReader(filePath);
			} else {
				reader = new FSFileSplitReader(getFilePath());
			}

			if (getQuoteChar() != null) {
				dummySchema = new TableSchema(new String[] {"_QUOTE_NUM_", "_SPLIT_NUMBER_", "_SPLIT_INFO_"},
					new TypeInformation[] {Types.LONG, Types.LONG, Types.STRING});

				splits = execEnv
					.createInput(reader.convertFileSplitToInputFormat(rowDelim, ignoreFirstLine, quoteChar),
						new RowTypeInfo(dummySchema.getFieldTypes(), dummySchema.getFieldNames()))
					.name("csv_split_summary_source");

				rows = splits.flatMap(new RichFlatMapFunction <Row, Row>() {
					boolean[] filedInQuote;

					@Override
					public void open(Configuration parameters) throws Exception {
						super.open(parameters);

						List <Row> splits = getRuntimeContext().getBroadcastVariable("splits");

						filedInQuote = new boolean[splits.size()];
						long[] sum = new long[splits.size()];

						for (Row row : splits) {
							long splitNum = (long) row.getField(1);
							sum[(int) splitNum] = (long) row.getField(0);
						}

						filedInQuote[0] = false;

						for (int i = 1; i < sum.length; i++) {
							sum[i] = sum[i - 1] + sum[i];
							filedInQuote[i] = sum[i - 1] % 2 == 1;
						}
					}

					@Override
					public void flatMap(Row value, Collector <Row> out) throws Exception {
						long splitNum = (long) value.getField(1);
						String splitStr = (String) value.getField(2);
						InputSplit split = reader.convertStringToSplitObject(splitStr);

						GenericCsvInputFormatBeta inputFormat = reader.getInputFormat(rowDelim, ignoreFirstLine,
							quoteChar);
						inputFormat.setFieldInQuote(filedInQuote[(int) splitNum]);
						inputFormat.open(split);

						while (true) {
							Row line = inputFormat.nextRecord(null);
							if (line == null) {
								break;
							}
							out.collect(line);
						}
					}
				}).withBroadcastSet(splits, "splits").name("csv_flat_map");
			} else {
				dummySchema = new TableSchema(new String[] {"f1"}, new TypeInformation[] {Types.STRING});

				rows = execEnv
					.createInput(reader.getInputFormat(rowDelim, ignoreFirstLine, quoteChar),
						new RowTypeInfo(dummySchema.getFieldTypes(), dummySchema.getFieldNames()))
					.name("csv_source");
			}
		} else {
			dummySchema = new TableSchema(new String[] {"f1"}, new TypeInformation[] {Types.STRING});

			Tuple2 <DataSet <Row>, TableSchema> schemaAndData;

			try {
				schemaAndData = DataSetUtil.readFromPartitionBatch(
					getParams(), getMLEnvironmentId(),
					new CsvSourceCollectorCreator(dummySchema, rowDelim, ignoreFirstLine, quoteChar)
				);
			} catch (IOException e) {
				throw new AkIllegalDataException(
					String.format("Fail to list directories in %s and select partitions", getFilePath().getPathStr()));
			}

			rows = schemaAndData.f0;
		}

		rows = rows.flatMap(new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine, lenient));

		return DataSetConversionUtil.toTable(getMLEnvironmentId(), rows, colNames, colTypes);
	}
}
