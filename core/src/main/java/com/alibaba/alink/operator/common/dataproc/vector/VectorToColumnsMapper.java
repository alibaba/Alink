package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * This mapper maps vector to table columns.
 */
public class VectorToColumnsMapper extends Mapper {
	private int colSize;
	private int idx;
	private OutputColsHelper outputColsHelper;

    /**
     * Constructor.
     * @param dataSchema the dataSchema.
     * @param params     the params.
     */
	public VectorToColumnsMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String selectedColName = this.params.get(VectorToColumnsParams.SELECTED_COL);
		idx = TableUtil.findColIndex(dataSchema.getFieldNames(), selectedColName);
		Preconditions.checkArgument(idx >= 0, "Can not find column: " + selectedColName);
		String[] outputColNames = this.params.get(VectorToColumnsParams.OUTPUT_COLS);
		Preconditions.checkArgument(null != outputColNames,
				"VectorToTable: outputColNames must set.");
		this.colSize = outputColNames.length;
		TypeInformation[] types = new TypeInformation[colSize];
		Arrays.fill(types, Types.DOUBLE);
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColNames, types,
			this.params.get(VectorToColumnsParams.RESERVED_COLS));
	}

    /**
     * The operation function to transform vector to table columns.
     * @param row the input Row type data
     * @return the output row.
     */
	@Override
	public Row map(Row row) {
		if (null == row) {
			return null;
		}
		Row result = new Row(colSize);
		Object obj = row.getField(idx);
		if (null == obj) {
			for (int i = 0; i < colSize; i++) {
				result.setField(i, null);
			}
			return outputColsHelper.getResultRow(row, result);
		}

		Vector vec = VectorUtil.getVector(obj);

		if (vec instanceof SparseVector) {
			for (int i = 0; i < colSize; ++i) {
				result.setField(i, 0.0);
			}
			SparseVector sparseVector = (SparseVector) vec;
			int nnz = sparseVector.numberOfValues();
			int[] indices = sparseVector.getIndices();
			double[] values = sparseVector.getValues();
			for (int i = 0; i < nnz; ++i) {
				if (indices[i] < colSize) {
					result.setField(indices[i], values[i]);
				} else {
					break;
				}
			}
		} else {
			DenseVector denseVector = (DenseVector) vec;
			for (int i = 0; i < colSize; ++i) {
				result.setField(i, denseVector.get(i));
			}
		}
		return outputColsHelper.getResultRow(row, result);
	}

	/**
	 * Get the output data schema.
	 */
	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}
}
