package com.alibaba.alink.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Abstract class for mappers.
 */
public abstract class Mapper implements Serializable {

	/**
	 * schema of the input.
	 */
	private final String[] dataFieldNames;
	private final DataType[] dataFieldTypes;

	/**
	 * params used for Mapper.
	 * User can set the params before that the Mapper is executed.
	 */
	protected final Params params;

	public Mapper(TableSchema dataSchema, Params params) {
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();
	}

	protected TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * map operation method that maps a row to a new row.
	 *
	 * @param row the input Row type data
	 * @return one Row type data
	 * @throws Exception This method may throw exceptions. Throwing
	 * an exception will cause the operation to fail.
	 */
	public abstract Row map(Row row) throws Exception;

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public abstract TableSchema getOutputSchema();

}
