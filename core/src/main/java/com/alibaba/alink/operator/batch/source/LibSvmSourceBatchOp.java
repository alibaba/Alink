package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.source.LibSvmSourceLocalOp;
import com.alibaba.alink.params.io.LibSvmSourceParams;

/**
 * A data source that reads libsvm format data.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SourceBatch)
@NameCn("LibSvm文件读入")
@NameEn("LibSvm Source")
public final class LibSvmSourceBatchOp extends BaseSourceBatchOp <LibSvmSourceBatchOp>
	implements LibSvmSourceParams <LibSvmSourceBatchOp> {

	private static final long serialVersionUID = -5424376385045080942L;

	public LibSvmSourceBatchOp() {
		this(new Params());
	}

	public LibSvmSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(LibSvmSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		BatchOperator <?> source = new CsvSourceBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setSchemaStr("content string")
			.setPartitions(getPartitions());

		final int startIndex = getParams().get(LibSvmSourceParams.START_INDEX);

		DataSet <Row> data = source.getDataSet()
			.map(new MapFunction <Row, Row>() {
				private static final long serialVersionUID = 2844973160952262773L;

				@Override
				public Row map(Row value) {
					String line = (String) value.getField(0);
					Tuple2 <Double, Vector> labelAndFeatures = LibSvmSourceLocalOp.parseLibSvmFormat(line, startIndex);
					return Row.of(labelAndFeatures.f0, labelAndFeatures.f1);
				}
			});
		return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, LibSvmSourceLocalOp.LIB_SVM_TABLE_SCHEMA);
	}
}
