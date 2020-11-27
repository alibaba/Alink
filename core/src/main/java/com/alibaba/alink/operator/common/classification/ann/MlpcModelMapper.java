package com.alibaba.alink.operator.common.classification.ann;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ModelMapper for {@link com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel}.
 */
public class MlpcModelMapper extends RichModelMapper {

	private static final long serialVersionUID = 2691422221337359053L;
	private boolean isVectorInput;
	private int vectorColIdx;
	private int[] featureColIdx;
	private transient TopologyModel topo;
	private transient List <Object> labels;
	private int vecSize;

	public MlpcModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	public static DenseVector getFeaturesVector(Row row, boolean isVectorInput,
												int[] featureColIdx, int vectorColIdx, int vecSize) {
		if (isVectorInput) {
			Vector vec = VectorUtil.getVector(row.getField(vectorColIdx));
			if (null == vec) {
				return null;
			} else {
				if (vec instanceof DenseVector) {
					return (DenseVector) vec;
				} else {
					SparseVector tmpVec = (SparseVector) vec;
					tmpVec.setSize(vecSize);
					return tmpVec.toDenseVector();
				}
			}
		} else {
			int n = featureColIdx.length;
			DenseVector features = new DenseVector(n);
			for (int i = 0; i < n; i++) {
				double v = ((Number) row.getField(featureColIdx[i])).doubleValue();
				features.set(i, v);
			}
			return features;
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		MlpcModelData model = new MlpcModelDataConverter().load(modelRows);
		model.labelType = super.getModelSchema().getFieldTypes()[2];

		this.labels = model.labels;
		int[] layerSize0 = model.meta.get(MultilayerPerceptronTrainParams.LAYERS);
		this.vecSize = layerSize0[0];
		Topology topology = FeedForwardTopology.multiLayerPerceptron(layerSize0, true);
		this.topo = topology.getModel(model.weights);
		isVectorInput = model.meta.get(ModelParamName.IS_VECTOR_INPUT);

		TableSchema dataSchema = getDataSchema();
		if (isVectorInput) {
			String vectorColName = params.contains(MultilayerPerceptronPredictParams.VECTOR_COL) ?
				params.get(MultilayerPerceptronPredictParams.VECTOR_COL) :
				model.meta.get(MultilayerPerceptronPredictParams.VECTOR_COL);
			this.vectorColIdx = TableUtil.findColIndexWithAssert(dataSchema.getFieldNames(), vectorColName);
		} else {
			String[] featureColNames = model.meta.get(MultilayerPerceptronTrainParams.FEATURE_COLS);
			this.featureColIdx = TableUtil.findColIndicesWithAssert(dataSchema.getFieldNames(), featureColNames);
		}
	}

	@Override
	protected Object predictResult(Row row) throws Exception {
		return predictResultDetail(row).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(Row row) throws Exception {
		DenseVector x = getFeaturesVector(row, isVectorInput, featureColIdx, vectorColIdx, vecSize);
		DenseVector prob = this.topo.predict(x);
		Map <Comparable, Double> predDetailMap = new HashMap <>(prob.size());

		int argmax = -1;
		double maxProb = 0.;
		for (int i = 0; i < prob.size(); i++) {
			if (prob.get(i) > maxProb) {
				argmax = i;
				maxProb = prob.get(i);
			}
		}
		for (int i = 0; i < prob.size(); i++) {
			predDetailMap.put((Comparable) labels.get(i), prob.get(i));
		}
		return Tuple2.of(labels.get(argmax), JsonConverter.toJson(predDetailMap));
	}
}
