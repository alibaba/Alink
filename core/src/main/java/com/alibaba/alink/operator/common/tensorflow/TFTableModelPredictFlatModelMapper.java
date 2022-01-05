package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.mapper.FlatModelMapper;

import java.io.Serializable;
import java.util.List;

/**
 * {@link TFTableModelPredictFlatModelMapper} provides inference for SavedModel which is in Alink Model format.
 */
public class TFTableModelPredictFlatModelMapper extends FlatModelMapper implements Serializable {

	private final BaseTFSavedModelPredictRowFlatMapper mapper;

	public TFTableModelPredictFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		this(modelSchema, dataSchema, params, new TFPredictorClassLoaderFactory());
	}

	public TFTableModelPredictFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params,
											  TFPredictorClassLoaderFactory factory) {
		super(modelSchema, dataSchema, params);
		mapper = new BaseTFSavedModelPredictRowFlatMapper(dataSchema, params, factory);
	}

	@Override
	public TableSchema getOutputSchema() {
		return mapper.getOutputSchema();
	}

	@Override
	public void open() {
		mapper.open();
	}

	@Override
	public void close() {
		mapper.close();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		String path = TFSavedModelUtils.loadSavedModelFromRows(modelRows);
		mapper.setModelPath(path);
	}

	public void loadModel(Iterable <Row> modelRows) {
		String path = TFSavedModelUtils.loadSavedModelFromRows(modelRows);
		mapper.setModelPath(path);
	}

	public void loadModelFromZipFile(String zipFilePath) {
		String path = TFSavedModelUtils.loadSavedModelFromZipFile(zipFilePath);
		mapper.setModelPath(path);
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		mapper.flatMap(row, output);
	}
}
