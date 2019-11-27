package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * ModelMapper with Single Input column and Single Output column(SISO).
 */
public abstract class SISOModelMapper extends ModelMapper {

	/**
	 * The OutputColsHelper which helps to arrange final output from input and predicted result.
	 */
	private final SISOColsHelper colsHelper;

	public SISOModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.colsHelper = new SISOColsHelper(dataSchema, initPredResultColType(), params);
	}

	/**
	 * Determine the prediction result type of the {@link SISOModelMapper#predictResult(Object)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation initPredResultColType();

	/**
	 * Predict the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single predicted result.
	 * @throws Exception
	 */
	protected abstract Object predictResult(Object input) throws Exception;

	@Override
	public TableSchema getOutputSchema() {
		return colsHelper.getOutputSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.colsHelper.handleMap(row, this::predictResult);
	}
}
