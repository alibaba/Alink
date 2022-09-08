package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;

import java.io.Serializable;

/**
 * Abstract class for mappers.
 */
public abstract class Mapper implements Serializable {

	private static final long serialVersionUID = -3634328096241559957L;

	/**
	 * Field names of the input.
	 */
	private final String[] dataFieldNames;

	/**
	 * Field types of the input.
	 */
	private final DataType[] dataFieldTypes;

	/**
	 * params used for Mapper. User can set the params before that the Mapper is executed.
	 */
	protected final Params params;

	/**
	 * Tuple of selectedCols, resultCols, resultTypes, reservedCols.
	 */
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> ioSchema;

	/**
	 * Buffers for `map` and `bufferMap`
	 */
	private MemoryTransformer transformer;
	private SlicedSelectedSampleThreadLocal selection;
	private SlicedSlicedResultThreadLocal result;

	private SlicedSelectedSampleArrayThreadLocal selections;
	private SlicedSlicedResultArrayThreadLocal results;

	public Mapper(TableSchema dataSchema, Params params) {
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();

		ioSchema = prepareIoSchema(dataSchema, params);

		checkIoSchema();

		initializeSliced();
	}

	public void open() {
	}

	public void close() {
	}

	/**
	 * map operation method that maps a row to a new row.
	 *
	 * @param row the input Row type data
	 * @return one Row type data
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	public Row map(Row row) throws Exception {
		Row output = new Row(getOutputSchema().getFieldNames().length);

		transformer.transform(row, output);

		SlicedSelectedSample selection = this.selection.get();
		SlicedResult result = this.result.get();

		selection.resetInstance(row);
		result.resetInstance(output);

		map(selection, result);

		return output;
	}

	public Row[] bunchMap(Row[] rows) throws Exception {
		Row[] outs = new Row[rows.length];
		for (int i = 0; i < rows.length; i++) {
			outs[i] = new Row(getOutputSchema().getFieldNames().length);
		}
		map(rows, outs, rows.length);
		return outs;
	}

	public void map(Row[] rows, Row[] outs, int len) throws Exception {
		if (outs == null) {
			outs = new Row[len];
			for (int i = 0; i < len; i++) {
				outs[i] = new Row(getOutputSchema().getFieldNames().length);
			}
		}
		if (outs[0] == null) {
			for (int i = 0; i < len; i++) {
				outs[i] = new Row(getOutputSchema().getFieldNames().length);
			}
		}

		if (selections == null) {
			this.selections = new SlicedSelectedSampleArrayThreadLocal(
				TableUtil.findColIndicesWithAssertAndHint(getDataSchema(), ioSchema.f0), len);
		}

		if (this.results == null) {
			this.results = new SlicedSlicedResultArrayThreadLocal(
				TableUtil.findColIndicesWithAssertAndHint(getOutputSchema(), ioSchema.f1), len);
		}

		for (int i = 0; i < len; i++) {
			transformer.transform(rows[i], outs[i]);
		}

		SlicedSelectedSample[] selectionArray = this.selections.get();
		SlicedResult[] resultArray = this.results.get();

		for (int i = 0; i < len; i++) {
			selectionArray[i].resetInstance(rows[i]);
			resultArray[i].resetInstance(outs[i]);
		}

		bunchMap(selectionArray, resultArray, len);
	}

	public void bufferMap(Row bufferRow, int[] bufferSelectedColIndices, int[] bufferResultColIndices)
		throws Exception {

		SlicedSelectedSample selection = this.selection.get();
		SlicedResult result = this.result.get();

		selection.resetColumnIndices(bufferSelectedColIndices);
		selection.resetInstance(bufferRow);

		result.resetColumnIndices(bufferResultColIndices);
		result.resetInstance(bufferRow);

		map(selection, result);
	}

	protected abstract void map(SlicedSelectedSample selection, SlicedResult result) throws Exception;

	protected void bunchMap(SlicedSelectedSample[] selections, SlicedResult[] results, int len) throws Exception {
		for (int i = 0; i < len; i++) {
			map(selections[i], results[i]);
		}
	}

	public final TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * Get the name of input selected columns.
	 */
	public final String[] getSelectedCols() {
		return ioSchema.f0;
	}

	/**
	 * Get the name of result column, i.e., without the reserved columns.
	 */
	public final String[] getResultCols() {
		return ioSchema.f1;
	}

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public TableSchema getOutputSchema() {

		OutputColsHelper outputColsHelper = new OutputColsHelper(
			getDataSchema(), ioSchema.f1, ioSchema.f2, ioSchema.f3
		);

		return outputColsHelper.getResultSchema();
	}

	/**
	 * Returns the tuple of selectedCols, resultCols, resultTypes, reservedCols.
	 */
	protected abstract Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params);

	protected static final class SlicedSelectedSample implements Serializable {

		private static final long serialVersionUID = 2774288987465829372L;

		private int[] columnIndices;
		private transient Row instance;

		private SlicedSelectedSample(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		public int length() {
			return columnIndices.length;
		}

		public Object get(int pos) {
			return instance.getField(columnIndices[pos]);
		}

		public void fillDenseVector(DenseVector dense, boolean hasInterceptItem) {
			fillDenseVector(dense, hasInterceptItem, null);
		}

		public void fillDenseVector(DenseVector dense, boolean hasInterceptItem, int[] quadraticColumnIndices) {
			if (hasInterceptItem && quadraticColumnIndices != null) {
				dense.set(0, 1.0);
				for (int i = 0, j = 1; i < quadraticColumnIndices.length; ++i, ++j) {
					if (get(quadraticColumnIndices[i]) instanceof Number) {
						dense.set(j, ((Number) get(quadraticColumnIndices[i])).doubleValue());
					} else {
						dense.set(j, 0.0);
					}
				}
				return;
			}

			if (hasInterceptItem) {
				dense.set(0, 1.0);
				for (int i = 0, j = 1; i < length(); ++i, ++j) {
					if (get(i) instanceof Number) {
						dense.set(j, ((Number) get(i)).doubleValue());
					} else {
						dense.set(j, 0.0);
					}
				}

				return;
			}

			if (quadraticColumnIndices != null) {
				for (int i = 0; i < quadraticColumnIndices.length; ++i) {
					if (get(quadraticColumnIndices[i]) instanceof Number) {
						dense.set(i, ((Number) get(quadraticColumnIndices[i])).doubleValue());
					} else {
						dense.set(i, 0.0);
					}
				}

				return;
			}

			for (int i = 0; i < length(); ++i) {
				if (get(i) instanceof Number) {
					dense.set(i, ((Number) get(i)).doubleValue());
				} else {
					dense.set(i, 0.0);
				}
			}
		}

		public void fillRow(Row row) {
			for (int i = 0; i < length(); ++i) {
				row.setField(i, get(i));
			}
		}

		void resetColumnIndices(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		void resetInstance(Row instance) {
			this.instance = instance;
		}
	}

	protected static final class SlicedResult implements Serializable {

		private static final long serialVersionUID = -702606581670930534L;

		private int[] columnIndices;
		private transient Row instance;

		private SlicedResult(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		public int length() {
			return columnIndices.length;
		}

		public Object get(int pos) {
			return instance.getField(columnIndices[pos]);
		}

		public void set(int pos, Object value) {
			instance.setField(columnIndices[pos], value);
		}

		void resetColumnIndices(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		void resetInstance(Row instance) {
			this.instance = instance;
		}
	}

	protected static class SlicedSelectedSampleThreadLocal
		extends ThreadLocal <SlicedSelectedSample> implements Serializable {

		private static final long serialVersionUID = -880790266294357596L;
		private final int[] columnIndices;

		public SlicedSelectedSampleThreadLocal(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		@Override
		protected SlicedSelectedSample initialValue() {
			return new SlicedSelectedSample(columnIndices);
		}
	}

	protected static class SlicedSlicedResultThreadLocal
		extends ThreadLocal <SlicedResult> implements Serializable {

		private static final long serialVersionUID = 3929812061012978137L;

		private final int[] columnIndices;

		public SlicedSlicedResultThreadLocal(int[] columnIndices) {
			this.columnIndices = columnIndices;
		}

		@Override
		protected SlicedResult initialValue() {
			return new SlicedResult(columnIndices);
		}
	}

	protected static class SlicedSelectedSampleArrayThreadLocal
		extends ThreadLocal <SlicedSelectedSample[]> implements Serializable {

		private static final long serialVersionUID = -880790266294357596L;
		private final int[] columnIndices;
		private final int size;

		public SlicedSelectedSampleArrayThreadLocal(int[] columnIndices, int size) {
			this.columnIndices = columnIndices;
			this.size = size;
		}

		@Override
		protected SlicedSelectedSample[] initialValue() {
			SlicedSelectedSample[] samples = new SlicedSelectedSample[size];
			for (int i = 0; i < size; i++) {
				samples[i] = new SlicedSelectedSample(columnIndices);
			}
			return samples;
		}
	}

	protected static class SlicedSlicedResultArrayThreadLocal
		extends ThreadLocal <SlicedResult[]> implements Serializable {

		private static final long serialVersionUID = 3929812061012978137L;
		private final int[] columnIndices;
		private final int size;

		public SlicedSlicedResultArrayThreadLocal(int[] columnIndices, int size) {
			this.columnIndices = columnIndices;
			this.size = size;
		}

		@Override
		protected SlicedResult[] initialValue() {
			SlicedResult[] samples = new SlicedResult[size];
			for (int i = 0; i < size; i++) {
				samples[i] = new SlicedResult(columnIndices);
			}
			return samples;
		}
	}

	static class MemoryTransformer implements Serializable {
		private final int[] reservedSelectedIndices;
		private final int[] reservedResultIndices;

		public MemoryTransformer(int[] reservedSelectedIndices, int[] reservedResultIndices) {
			this.reservedSelectedIndices = reservedSelectedIndices;
			this.reservedResultIndices = reservedResultIndices;
		}

		public void transform(Row row, Row output) {
			for (int i = 0; i < reservedSelectedIndices.length; ++i) {
				output.setField(
					reservedResultIndices[i],
					row.getField(reservedSelectedIndices[i])
				);
			}
		}
	}

	protected final void initializeSliced() {

		if (null != ioSchema) {

			OutputColsHelper outputColsHelper = new OutputColsHelper(
				getDataSchema(), ioSchema.f1, ioSchema.f2, ioSchema.f3
			);

			this.transformer = new MemoryTransformer(
				TableUtil.findColIndicesWithAssertAndHint(getDataSchema(), outputColsHelper.getReservedColumns()),
				TableUtil.findColIndicesWithAssertAndHint(getOutputSchema(), outputColsHelper.getReservedColumns())
			);

			this.selection = new SlicedSelectedSampleThreadLocal(
				TableUtil.findColIndicesWithAssertAndHint(getDataSchema(), ioSchema.f0)
			);

			this.result = new SlicedSlicedResultThreadLocal(
				TableUtil.findColIndicesWithAssertAndHint(getOutputSchema(), ioSchema.f1)
			);

		}
	}

	protected final void checkIoSchema() {

		if (null != ioSchema) {
			AkPreconditions.checkState(
				ioSchema.f0 != null,
				"Selected columns in mapper should not be null."
			);
			AkPreconditions.checkState(
				ioSchema.f1 != null,
				"Output columns in mapper should not be null."
			);
			AkPreconditions.checkState(
				ioSchema.f2 != null,
				"Output types in mapper should not be null."
			);
		}
	}
}
