package com.alibaba.alink.operator.local.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.params.io.LibSvmSinkBatchParams;
import com.alibaba.alink.params.io.LibSvmSinkParams;

/**
 * Sink the data to files in libsvm format.
 */
@NameCn("LibSvm文件导出")
public final class LibSvmSinkLocalOp extends BaseSinkLocalOp <LibSvmSinkLocalOp>
	implements LibSvmSinkBatchParams <LibSvmSinkLocalOp> {

	public LibSvmSinkLocalOp() {
		this(new Params());
	}

	public LibSvmSinkLocalOp(Params params) {
		super(params);
	}

	public static String formatLibSvm(Object label, Object vector, int startIndex) {
		String labelStr = "";
		if (label != null) {
			labelStr = String.valueOf(label);
		}
		String vectorStr = "";
		if (vector != null) {
			if (vector instanceof String) {
				if (((String) vector).startsWith(("[")) && ((String) vector).endsWith("]")) {
					vector = ((String) vector).substring(1, ((String) vector).length() - 1);
				}
			}
			Vector v = VectorUtil.getVector(vector);
			if (v instanceof DenseVector) {
				v = toSparseVector((DenseVector) v);
			}
			int[] indices = ((SparseVector) v).getIndices();
			for (int i = 0; i < indices.length; i++) {
				indices[i] = indices[i] + startIndex;
			}
			vectorStr = VectorUtil.serialize(v);
		}
		return labelStr + " " + vectorStr;
	}

	private static SparseVector toSparseVector(DenseVector v) {
		int[] indices = new int[v.size()];
		double[] values = v.getData();
		for (int i = 0; i < indices.length; i++) {
			indices[i] = i;
		}
		return new SparseVector(-1, indices, values);
	}

	@Override
	public LibSvmSinkLocalOp sinkFrom(LocalOperator <?> in) {
		final String vectorCol = getVectorCol();
		final String labelCol = getLabelCol();

		final int vectorColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorCol);
		final int labelColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), labelCol);
		final int startIndex = getParams().get(LibSvmSinkParams.START_INDEX);

		RowCollector rowCollector = new RowCollector();
		for (Row value : in.getOutputTable().getRows()) {
			rowCollector.collect(Row.of(
				formatLibSvm(value.getField(labelColIdx), value.getField(vectorColIdx), startIndex)
			));
		}

		new MemSourceLocalOp(rowCollector.getRows(), "f string")
			.link(
				new CsvSinkLocalOp()
					.setQuoteChar(null)
					.setFilePath(getFilePath())
					.setOverwriteSink(getOverwriteSink())
					.setFieldDelimiter(" ")
					.setPartitionCols(getPartitionCols())
			);

		return this;
	}
}
