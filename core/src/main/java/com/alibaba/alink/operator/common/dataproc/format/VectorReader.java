package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;

import java.util.Map;

public class VectorReader extends FormatReader {

	final int vecColIndex;
	final String[] colNames;

	public VectorReader(int vecColIndex, TableSchema schema) {
		this.vecColIndex = vecColIndex;
		if (null == schema) {
			this.colNames = null;
		} else {
			this.colNames = schema.getFieldNames();
		}
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
				int nCol = Math.min(colNames.length, denseVector.size());
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
