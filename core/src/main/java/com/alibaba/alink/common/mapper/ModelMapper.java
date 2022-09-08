package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
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

		ioSchema = prepareIoSchema(modelSchema, dataSchema, params);

		checkIoSchema();

		initializeSliced();
	}

	protected final TableSchema getModelSchema() {
		return TableSchema.builder().fields(this.modelFieldNames, this.modelFieldTypes).build();
	}

	/**
	 * Note: Can not be called by the subclasses of ModelMapper.
	 */
	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {
		return null;
	}

	protected abstract Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params);

	public ModelMapper createNew(List<Row> newModelRows) {
		ModelMapper newModelMapper = reflectCreate();
		newModelMapper.loadModel(newModelRows);
		return newModelMapper;
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List <Row> modelRows);

	private ModelMapper reflectCreate() {

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

			throw new AkUnclassifiedErrorException("Error. ", e);
		}
	}
}
