package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;

import java.io.Serializable;
import java.util.List;

/**
 * {@link TFTableModelPredictModelMapper} provides inference for SavedModel which is in Alink Model format.
 */
public class TFTableModelPredictModelMapper extends ModelMapper implements Serializable {

	private final BaseTFSavedModelPredictRowMapper mapper;

	public TFTableModelPredictModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		mapper = new BaseTFSavedModelPredictRowMapper(dataSchema, params);
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

	public void loadModelFromZipFile(String zipFilePath) {
		String path = TFSavedModelUtils.loadSavedModelFromZipFile(zipFilePath);
		mapper.setModelPath(path);
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		return super.createNew(newModelRows);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] tfInputCols = params.get(TFTableModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		String tfOutputSchemaStr = params.get(TFTableModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = CsvUtil.schemaStr2Schema(tfOutputSchemaStr);
		String[] reservedCols = params.get(TFTableModelPredictParams.RESERVED_COLS);

		return Tuple4.of(tfInputCols,
			tfOutputSchema.getFieldNames(),
			tfOutputSchema.getFieldTypes(),
			reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapper.map(selection, result);
	}
}
