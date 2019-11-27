package com.alibaba.alink.operator.common.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.ScalerUtil;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.regression.IsotonicRegTrainParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.List;

/**
 * This mapper predicts the isotonic regression result.
 */
public class IsotonicRegressionModelMapper extends ModelMapper {
	private int colIdx;
	private IsotonicRegressionModelData modelData;
	private String vectorColName;
	private int featureIndex;

	/**
	 * Constructor.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public IsotonicRegressionModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data.
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		IsotonicRegressionConverter converter = new IsotonicRegressionConverter();
		modelData = converter.load(modelRows);
		Params meta = modelData.meta;
		String featureColName = meta.get(IsotonicRegTrainParams.FEATURE_COL);
		vectorColName = meta.get(IsotonicRegTrainParams.VECTOR_COL);
		featureIndex = meta.get(IsotonicRegTrainParams.FEATURE_INDEX);
		TableSchema dataSchema = getDataSchema();
		if (null == vectorColName) {
			colIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), featureColName);
		} else {
			colIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), vectorColName);
		}
	}

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data.
	 */
	@Override
	public TableSchema getOutputSchema() {
		String predictResultColName = this.params.get(HasPredictionCol.PREDICTION_COL);
		TableSchema dataSchema = getDataSchema();
		return new TableSchema(
			ArrayUtils.add(dataSchema.getFieldNames(), predictResultColName),
			ArrayUtils.add(dataSchema.getFieldTypes(), Types.DOUBLE())
		);
	}

	/**
	 * Map operation method.
	 *
	 * @param row the input Row type data.
	 * @return one Row type data.
	 * @throws Exception This method may throw exceptions. Throwing
	 *                   an exception will cause the operation to fail.
	 */
	@Override
	public Row map(Row row) throws Exception {
		if (null == row) {
			return null;
		}
		if (null == row.getField(colIdx)) {
			return RowUtil.merge(row, (Object) null);
		}
		//use Binary search method to search for the boundaries.
		double feature = (null == this.vectorColName ? ((Number) row.getField(colIdx)).doubleValue() :
				VectorUtil.getVector(row.getField(colIdx)).get(this.featureIndex));
		int foundIndex = Arrays.binarySearch(modelData.boundaries, feature);
		int insertIndex = -foundIndex - 1;
		double predict;
		if (insertIndex == 0) {
			predict = modelData.values[0];
		} else if (insertIndex == modelData.boundaries.length) {
			predict = modelData.values[modelData.values.length - 1];
		} else if (foundIndex < 0) {
			predict = ScalerUtil.minMaxScaler(feature, modelData.boundaries[insertIndex - 1], modelData.boundaries[insertIndex],
					modelData.values[insertIndex], modelData.values[insertIndex - 1]);
		} else {
			predict = modelData.values[foundIndex];
		}
		return RowUtil.merge(row, predict);
	}
}
