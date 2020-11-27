package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.LibSvmSourceParams;

/**
 * A data source that reads libsvm format data.
 */
@IoOpAnnotation(name = "libsvm", ioType = IOType.SourceBatch)
public final class LibSvmSourceBatchOp extends BaseSourceBatchOp <LibSvmSourceBatchOp>
	implements LibSvmSourceParams <LibSvmSourceBatchOp> {

	private static final long serialVersionUID = -5424376385045080942L;

	public LibSvmSourceBatchOp() {
		this(new Params());
	}

	public LibSvmSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(LibSvmSourceBatchOp.class), params);
	}

	public static Tuple2 <Double, Vector> parseLibSvmFormat(String line, int startIndex) {
		if (StringUtils.isNullOrWhitespaceOnly(line)) {
			return Tuple2.of(null, null);
		}
		int firstSpacePos = line.indexOf(' ');
		if (firstSpacePos < 0) {
			return Tuple2.of(Double.valueOf(line), VectorUtil.getVector(""));
		}
		String labelStr = line.substring(0, firstSpacePos);
		String featuresStr = line.substring(firstSpacePos + 1);
		Vector featuresVec = VectorUtil.getVector(featuresStr);
		if (featuresVec instanceof SparseVector) {
			int[] indices = ((SparseVector) featuresVec).getIndices();
			for (int i = 0; i < indices.length; i++) {
				indices[i] = indices[i] - startIndex;
			}
		}
		return Tuple2.of(Double.valueOf(labelStr), featuresVec);
	}

	public static final TableSchema LIB_SVM_TABLE_SCHEMA = new TableSchema(new String[] {"label", "features"},
		new TypeInformation[] {Types.DOUBLE(), VectorTypes.VECTOR});

	@Override
	public Table initializeDataSource() {
		BatchOperator<?> source = new CsvSourceBatchOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setSchemaStr("content string");

		final int startIndex = getParams().get(LibSvmSourceParams.START_INDEX);

		DataSet <Row> data = source.getDataSet()
			.map(new MapFunction <Row, Row>() {
				private static final long serialVersionUID = 2844973160952262773L;

				@Override
				public Row map(Row value) throws Exception {
					String line = (String) value.getField(0);
					Tuple2 <Double, Vector> labelAndFeatures = parseLibSvmFormat(line, startIndex);
					return Row.of(labelAndFeatures.f0, labelAndFeatures.f1);
				}
			});
		return DataSetConversionUtil.toTable(getMLEnvironmentId(), data, LIB_SVM_TABLE_SCHEMA);
	}
}
