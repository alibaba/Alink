package com.alibaba.alink.operator.common.tree.predictors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.XGboostException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.outlier.OutlierUtil;
import com.alibaba.alink.operator.common.tree.XGBoostModelDataConverter;
import com.alibaba.alink.operator.common.tree.xgboost.Booster;
import com.alibaba.alink.operator.common.tree.xgboost.XGBoost;
import com.alibaba.alink.operator.common.tree.xgboost.plugin.XGBoostClassLoaderFactory;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.xgboost.HasObjective.Objective;
import com.alibaba.alink.params.xgboost.XGBoostInputParams;
import com.alibaba.alink.params.xgboost.XGBoostLearningTaskParams;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class XGBoostModelMapper extends RichModelMapper {

	private final static float[] INITIAL_VALUE_OF_LABEL = new float[] {0.0f};

	private final XGBoostClassLoaderFactory xgBoostClassLoaderFactory;

	private transient Booster booster;
	private transient int vectorColIndex;
	private transient Object[] labels;
	private transient int vectorSize;
	private transient Objective objective;
	private transient Function <Row, Row> selectFieldsFunction;

	public XGBoostModelMapper(
		TableSchema modelSchema,
		TableSchema dataSchema,
		Params params) {

		super(modelSchema, dataSchema, params);

		xgBoostClassLoaderFactory = new XGBoostClassLoaderFactory(
			params.get(XGBoostPredictParams.PLUGIN_VERSION)
		);
	}

	@Override
	public void loadModel(List <Row> modelRows) {

		final XGBoostModelDataConverter xgBoostModelDataConverter = new XGBoostModelDataConverter();
		xgBoostModelDataConverter.load(modelRows);

		labels = xgBoostModelDataConverter.labels;

		XGBoost xgBoost = XGBoostClassLoaderFactory.create(xgBoostClassLoaderFactory).create();

		try {
			booster = xgBoost.loadModel(new InputStream() {
				private final Decoder base64Encoder = Base64.getDecoder();
				private final Iterator <String> iterator
					= xgBoostModelDataConverter.modelData.iterator();

				private byte[] buffer;
				private int cursor = 0;

				@Override
				public int read() {
					if ((buffer == null || cursor >= buffer.length) && iterator.hasNext()) {
						buffer = base64Encoder.decode(iterator.next());
						cursor = 0;
					}

					return buffer == null || cursor >= buffer.length ? -1 : buffer[cursor++] & 255;
				}
			});
		} catch (XGboostException | IOException xgBoostError) {
			throw new IllegalStateException(xgBoostError);
		}

		vectorSize = xgBoostModelDataConverter.meta.get(XGBoostModelDataConverter.XGBOOST_VECTOR_SIZE);

		objective = xgBoostModelDataConverter.meta.get(XGBoostLearningTaskParams.OBJECTIVE);

		if (xgBoostModelDataConverter.meta.contains(XGBoostInputParams.VECTOR_COL)) {
			final int localVectorColIndex = TableUtil.findColIndexWithAssertAndHint(
				getDataSchema(), xgBoostModelDataConverter.meta.get(HasVectorCol.VECTOR_COL)
			);

			selectFieldsFunction = row -> Row.of(row.getField(localVectorColIndex));
		} else {
			final String[] featureCols = OutlierUtil.uniformFeatureColsDefaultAsAll(
				xgBoostModelDataConverter.meta.get(XGBoostInputParams.FEATURE_COLS),
				TableUtil.getNumericCols(getDataSchema())
			);

			final int[] featureColIndices = TableUtil.findColIndicesWithAssertAndHint(
				getDataSchema(), featureCols
			);

			selectFieldsFunction = row -> Row.of(
				OutlierUtil.rowToDenseVector(row, featureColIndices, featureColIndices.length));
		}

		vectorColIndex = 0;
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Row localRow = new Row(selection.length());
		selection.fillRow(localRow);

		float[] result = booster.predict(
			localRow,
			selectFieldsFunction,
			row -> {
				Vector vector = VectorUtil.getVector(row.getField(vectorColIndex));

				if (vector instanceof SparseVector && vector.size() < 0) {
					((SparseVector) vector).setSize(vectorSize);
				}

				return Tuple3.of(vector, INITIAL_VALUE_OF_LABEL, 1.0f);
			}
		);

		if (result == null || result.length <= 0) {
			return Tuple2.of(null, null);
		}

		switch (objective) {
			case BINARY_LOGISTIC: {
				Map <Object, Double> probability = new HashMap <>();
				probability.put(labels[0], 1.0 - (double) result[0]);
				probability.put(labels[1], (double) result[0]);
				Object obj = result[0] > 0.5f ? labels[1] : labels[0];
				return Tuple2.of(obj, JsonConverter.toJson(probability));
			}
			case BINARY_HINGE: {
				Map <Object, Double> probability = new HashMap <>();
				probability.put(labels[0], result[0] == 1.0 ? 0.0 : 1.0);
				probability.put(labels[1], result[0] == 1.0 ? 1.0 : 0.0);
				Object obj = result[0] == 1.0 ? labels[1] : labels[0];
				return Tuple2.of(obj, JsonConverter.toJson(probability));
			}
			case MULTI_SOFTMAX: {
				Map <Object, Double> probability = new HashMap <>();

				for (int i = 0; i < labels.length; ++i) {
					if (result[0] == i) {
						probability.put(labels[i], 1.0);
					} else {
						probability.put(labels[i], 0.0);
					}
				}

				Object obj = labels[(int) result[0]];

				return Tuple2.of(obj, JsonConverter.toJson(probability));
			}
			case MULTI_SOFTPROB: {
				Map <Object, Double> probability = new HashMap <>();
				double max = 0.0;
				int maxIndex = 0;

				for (int i = 0; i < labels.length; ++i) {
					probability.put(labels[i], (double) result[i]);

					if (max > result[i]) {
						max = result[i];
						maxIndex = i;
					}
				}

				Object obj = labels[maxIndex];

				return Tuple2.of(obj, JsonConverter.toJson(probability));
			}
			default:
				return Tuple2.of((double) result[0], null);
		}
	}
}
