package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.Functional.ExceptionBiConsumer;
import com.alibaba.alink.common.utils.Functional.ExceptionFunction;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.mapper.SISOMapperParams;

import java.io.Serializable;

/**
 * A helper which helps to handle SISO mappers and flatmappers with a {@link SISOColsHelper} inside.
 */
class SISOColsHelper implements Serializable {

	/**
	 * The OutputColsHelper which helps to arrange final output from input and predicted result.
	 */
	private OutputColsHelper outputColsHelper;

	/**
	 * Index of input column in a row.
	 */
	private int colIndex;

	/**
	 * Create a SISOColsHelper object.
	 *
	 * <p>The selected column must appear in input data, or an  {@code IllegalArgumentException} will be thrown.
	 * If the output columns are not specified via params, it will be set to same as selected ones.
	 *
	 * @param dataSchema TableSchema of input rows.
	 * @param outputType Type of the single output column.
	 * @param params     Params to retrieve selected/output/reserved columns.
	 */
	SISOColsHelper(TableSchema dataSchema, TypeInformation<?> outputType, Params params) {
		String selectedColName = params.get(SISOMapperParams.SELECTED_COL);
		String outputColName = params.get(SISOMapperParams.OUTPUT_COL);
		String[] reservedColNames = params.get(SISOMapperParams.RESERVED_COLS);

		this.colIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), selectedColName);
		if (this.colIndex < 0) {
			throw new IllegalArgumentException("Can't find column " + selectedColName);
		}
		if (outputColName == null) {
			outputColName = selectedColName;
		}
		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColName, outputType, reservedColNames);
	}

	/**
	 * Get output schema by {@link OutputColsHelper}.
	 *
	 * @return The output schema.
	 */
	TableSchema getOutputSchema() {
		return this.outputColsHelper.getResultSchema();
	}

	/**
	 * Handle mappers.
	 *
	 * @param input input row.
	 * @param func  the function mapping from single column to single column.
	 * @return the result row with un-effected columns set properly.
	 * @throws Exception
	 */
	Row handleMap(Row input, ExceptionFunction<Object, Object, Exception> func) throws Exception {
		Object output = func.apply(input.getField(this.colIndex));
		return this.outputColsHelper.getResultRow(input, Row.of(output));
	}

	/**
	 * Handle flat mappers.
	 *
	 * @param input     input row
	 * @param collector collector to accept result rows.
	 * @param func      the function mapping from single column to zero or more values.
	 * @throws Exception
	 */
	void handleFlatMap(
		Row input, Collector<Row> collector,
		ExceptionBiConsumer<Object, Collector<Object>, Exception> func) throws Exception {
		CollectorWrapper collectorWrapper = new CollectorWrapper(collector, input);
		func.accept(input.getField(this.colIndex), collectorWrapper);
	}

	/**
	 * A wrapped {@link Collector} which accepts a single column but feeds a row to underlying
	 * proxy {@link Collector} which accept {@link Row}. The row is create from the the accepted
	 * column object, with other columns handled property by {@link OutputColsHelper}.
	 */
	private class CollectorWrapper implements Collector<Object> {

		private final Collector<Row> proxy;
		private final Row row;

		CollectorWrapper(Collector<Row> proxy, Row row) {
			this.proxy = proxy;
			this.row = row;
		}

		@Override
		public void collect(Object record) {
			Row output = outputColsHelper.getResultRow(row, Row.of(record));
			this.proxy.collect(output);
		}

		@Override
		public void close() {
			this.proxy.close();
		}
	}

}
