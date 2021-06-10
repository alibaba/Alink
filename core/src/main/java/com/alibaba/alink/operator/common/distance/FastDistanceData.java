package com.alibaba.alink.operator.common.distance;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Base class to save the data for calculating distance fast. It has two derived classes: FastDistanceVectorData and
 * FastDistanceMatrixData. FastDistanceVectorData saves only one vector(dense or spase) and FastDistanceMatrixData
 * merges several
 * dense vectors in a matrix.
 */
public abstract class FastDistanceData implements Serializable, Cloneable {
	private static final long serialVersionUID = -6327346472723810463L;
	/**
	 * Save the extra info besides the vector. Each vector is related to one row. Thus, for FastDistanceVectorData, the
	 * length of <code>rows</code> is one. And for FastDistanceMatrixData, the length of <code>rows</code> is equal to
	 * the number of cols of <code>matrix</code>. Besides, the order of the rows are the same with the vectors.
	 */
	final Row[] rows;

	public Row[] getRows() {
		return rows;
	}

	FastDistanceData(Row[] rows) {
		this.rows = rows;
	}

	FastDistanceData(FastDistanceData fastDistanceData) {
		this.rows = null == fastDistanceData.rows ? null : fastDistanceData.rows.clone();
	}

	public static Row parseRowCompatible(Params params) {
		if (params == null) {
			return null;
		}

		Row row = params.getOrDefault("rows", Row.class, null);

		if (row == null) {
			return null;
		}

		if (row.getKind() == null) {
			LegacyRow legacyRow = params.getOrDefault("rows", LegacyRow.class, null);

			Object[] objects = legacyRow.getFields();

			row = Row.of(objects);
		}

		return row;
	}

	public static Row[] parseRowArrayCompatible(Params params) {
		if (params == null) {
			return null;
		}

		Row[] rows = params.get("rows", Row[].class);

		if (rows == null) {
			return null;
		}

		if (rows.length == 0) {
			return rows;
		}

		if (rows[0].getKind() == null) {
			LegacyRow[] legacyRow = params.get("rows", LegacyRow[].class);

			rows = new Row[legacyRow.length];

			for (int i = 0; i < legacyRow.length; ++i) {
				rows[i] = Row.of(legacyRow[i].getFields());
			}
		}

		return rows;
	}

	private static final class LegacyRow {
		private final Object[] fields;

		public LegacyRow(Object[] fields) {
			this.fields = fields;
		}

		public Object[] getFields() {
			return fields;
		}
	}
}
