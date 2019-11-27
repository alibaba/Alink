package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * Abstract class for mappers with Single Input column and Single Output column(SISO).
 */
public abstract class SISOMapper extends Mapper {

	/**
	 * The OutputColsHelper which helps to arrange final output from input and predicted result.
	 */
	private final SISOColsHelper colsHelper;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input tableSchema
	 * @param params     input parameters.
	 */
	public SISOMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.colsHelper = new SISOColsHelper(dataSchema, initOutputColType(), params);
	}

	/**
	 * Determine the return type of the {@link SISOMapper#map(Object)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation initOutputColType();

	/**
	 * Map the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single map result.
	 * @throws Exception
	 */
	protected abstract Object mapColumn(Object input) throws Exception;

	@Override
	public TableSchema getOutputSchema() {
		return colsHelper.getOutputSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return colsHelper.handleMap(row, this::mapColumn);
	}
}
