package com.alibaba.alink.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract class for mappers with model.
 *
 * <p>Note: the {@link ModelMapper#map(Row)} should be thread safe and
 * if the ModelMapper use the independence buffer between threads,
 * it should be override the mirror method to initial the buffer.
 */
public abstract class ModelMapper extends Mapper {
	private static final Logger LOG = LoggerFactory.getLogger(ModelMapper.class);
	private static final long serialVersionUID = 4025027560447017077L;
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

	public void open() {
	}

	public void close() {
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List <Row> modelRows);

	/**
	 * Return a copy of 'this' object that is used in multi-threaded prediction.
	 * A rule of thumb is to share model data with the mirrored object, but not
	 * to share runtime buffer.
	 *
	 * If the ModelMapper is thread-safe (no runtime buffer), then just return 'this' is enough.
	 */
	@Override
	protected ModelMapper mirror() {
		return this;
	}
}
