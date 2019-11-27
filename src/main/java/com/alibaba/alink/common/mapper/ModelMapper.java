package com.alibaba.alink.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Abstract class for mappers with model.
 */
public abstract class ModelMapper extends Mapper {

	/**
	 * Field names of the model.
	 */
	private final String[] modelFieldNames;

	/**
	 * Field types of the model.
	 */
	private final DataType[] modelFieldTypes;

	public ModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.modelFieldNames = modelSchema.getFieldNames();
		this.modelFieldTypes = modelSchema.getFieldDataTypes();
	}

	protected TableSchema getModelSchema() {
		return TableSchema.builder().fields(this.modelFieldNames, this.modelFieldTypes).build();
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List<Row> modelRows);
}
