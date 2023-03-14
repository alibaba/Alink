package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.copy.csv.TextOutputFormat;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FlattenCsvFromRow;
import com.alibaba.alink.operator.common.io.csv.CsvUtil.FormatCsvFunc;
import com.alibaba.alink.operator.common.io.partition.CsvSinkCollectorCreator;
import com.alibaba.alink.params.io.CsvSinkBatchParams;

/**
 * Sink to local or HDFS files in CSV format.
 */

@IoOpAnnotation(name = "csv", ioType = IOType.SinkBatch)
@NameCn("CSV文件导出")
@NameEn("Csv Sink")
public final class CsvSinkBatchOp extends BaseSinkBatchOp <CsvSinkBatchOp>
	implements CsvSinkBatchParams <CsvSinkBatchOp> {

	private static final long serialVersionUID = -4482188826277270154L;

	public CsvSinkBatchOp() {
		this(new Params());
	}

	public CsvSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(CsvSinkBatchOp.class), params);
	}

	@Override
	public CsvSinkBatchOp sinkFrom(BatchOperator <?> in) {
		if (getPartitionCols() != null) {

			DataSetUtil.partitionAndWriteFile(
				in,
				new CsvSinkCollectorCreator(
					new FormatCsvFunc(in.getColTypes(), getFieldDelimiter(), getRowDelimiter(), getQuoteChar()),
					new FlattenCsvFromRow(getRowDelimiter()),
					getRowDelimiter()
				),
				getParams()
			);

		} else {

			final String filePath = getFilePath().getPathStr();
			final String fieldDelim = getFieldDelimiter();
			final String rowDelim = getRowDelimiter();
			final int numFiles = getNumFiles();
			final TypeInformation <?>[] types = in.getColTypes();
			final Character quoteChar = getQuoteChar();

			FileSystem.WriteMode mode = FileSystem.WriteMode.NO_OVERWRITE;
			if (getOverwriteSink()) {
				mode = FileSystem.WriteMode.OVERWRITE;
			}

			DataSet <String> textLines = in.getDataSet()
				.map(new FormatCsvFunc(types, fieldDelim, rowDelim, quoteChar))
				.map(new FlattenCsvFromRow(getRowDelimiter()));

			TextOutputFormat <String> tof = new TextOutputFormat <>(
				new Path(filePath), getFilePath().getFileSystem(), getRowDelimiter()
			);

			tof.setWriteMode(mode);

			textLines.output(tof).name("csv_sink").setParallelism(numFiles);
		}

		return this;
	}

}
