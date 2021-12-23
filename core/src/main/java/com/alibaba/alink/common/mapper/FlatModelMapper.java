package com.alibaba.alink.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Abstract class for flatMappers with model.
 *
 * <p>The general process of transform the input use machine learning model is:
 * <ul>
 * <li>1. load the model into memory.</li>
 * <li>2. process the input using the model.</li>
 * </ul>
 * So, different from the {@link FlatMapper}, this class has a new abstract method
 * named {@link #loadModel(List)} that load the model and transform it to the
 * memory structure.
 *
 * <p>The model is the machine learning model that use the Table as
 * its representation(serialized to Table from the memory
 * or deserialized from Table to memory).
 */
public abstract class FlatModelMapper extends FlatMapper {

	private static final long serialVersionUID = 4400386085689710022L;
	/**
	 * Field names of the model.
	 */
	private final String[] modelFieldNames;

	/**
	 * Field types of the model.
	 */
	private final DataType[] modelFieldTypes;

	public FlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
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

	public FlatModelMapper createNew(List<Row> newModelRows) {
		FlatModelMapper newFlatModelMapper = reflectCreate();
		newFlatModelMapper.loadModel(newModelRows);
		return newFlatModelMapper;
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List <Row> modelRows);

	private FlatModelMapper reflectCreate() {

		try {
			return getClass()
				.getConstructor(TableSchema.class, TableSchema.class, Params.class)
				.newInstance(
					getModelSchema(),
					getDataSchema(),
					params.clone()
				);
		} catch (NoSuchMethodException
			| IllegalAccessException
			| InstantiationException
			| InvocationTargetException e) {

			throw new RuntimeException(e);
		}
	}
}
