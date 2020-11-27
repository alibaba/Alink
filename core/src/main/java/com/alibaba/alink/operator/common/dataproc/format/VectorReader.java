package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Map;

public class VectorReader extends FormatReader {

	private static final long serialVersionUID = 2541314969442348764L;
	private final int vecColIndex;
	private final String[] colNames;
	private HasHandleInvalidDefaultAsError.HandleInvalid handleInvalid;

	public VectorReader(int vecColIndex, TableSchema schema,
						HasHandleInvalidDefaultAsError.HandleInvalid handleInvalid) {
		this.vecColIndex = vecColIndex;
		if (null == schema) {
			this.colNames = null;
		} else {
			this.colNames = schema.getFieldNames();
		}
		this.handleInvalid = handleInvalid;
	}

	@Override
	boolean read(Row row, Map <String, String> out) {
		Vector vec = VectorUtil.getVector(row.getField(vecColIndex));
		if (vec instanceof DenseVector) {
			DenseVector denseVector = (DenseVector) vec;
			if (null == colNames) {
				for (int i = 0; i < denseVector.size(); i++) {
					out.put(String.valueOf(i), String.valueOf(denseVector.get(i)));
				}
			} else {
				if (colNames.length > denseVector.size()) {
					if (handleInvalid.equals(HasHandleInvalidDefaultAsError.HandleInvalid.ERROR)) {
						String failMsg = String
							.format("colSize is larger than vector size! colSize: %s, vectorSize: %s", denseVector.size(), colNames.length);
						throw new RuntimeException(failMsg);
					}
					return false;
				}
				for (int i = 0; i < denseVector.size(); i++) {
					out.put(colNames[i], String.valueOf(denseVector.get(i)));
				}
			}
		} else {
			SparseVector sparseVector = (SparseVector) vec;
			if (null == colNames) {
				for (int i : sparseVector.getIndices()) {
					out.put(String.valueOf(i), String.valueOf(sparseVector.get(i)));
				}
			} else {
				if (sparseVector.size() != -1 && sparseVector.size() < colNames.length) {
					if (handleInvalid.equals(HasHandleInvalidDefaultAsError.HandleInvalid.ERROR)) {
						String failMsg = String
							.format("colSize is larger than vector size! colSize: %s, vectorSize: %s", sparseVector.size(), colNames.length);
						throw new RuntimeException(failMsg);
					}
					return false;
				}
				for (int i : sparseVector.getIndices()) {
					if (i < colNames.length) {
						out.put(colNames[i], String.valueOf(sparseVector.get(i)));
					}
				}
			}

		}
		return true;
	}
}
