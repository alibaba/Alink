package com.alibaba.alink.common.mapper;

import java.io.Serializable;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Abstract class for flatMappers.
 * FlatMapper maps a row to zero, one or multiple rows.
 */
public abstract class FlatMapper implements Serializable {

    /**
     * schema of the input.
     */
	private final String[] dataFieldNames;
	private final DataType[] dataFieldTypes;

    /**
     * params used for FlatMapper.
     * User can set the params before that the FlatMapper is executed.
     */
	protected Params params;

	public FlatMapper(TableSchema dataSchema, Params params) {
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();
	}

	protected TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * The core method of the FlatMapper.
	 * Takes a row from the input and maps it to multiple rows.
	 *
	 * @param row    The input row.
	 * @param output The collector for returning the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 * to fail.
	 */
	public abstract void flatMap(Row row, Collector <Row> output) throws Exception;

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public abstract TableSchema getOutputSchema();

}
