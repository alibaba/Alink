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

import java.util.Arrays;
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
	String[] featureColNames;
	private transient ThreadLocal <DenseVector> threadLocalVec;

	public MlpcModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	private void getFeaturesVector(SlicedSelectedSample selection, boolean isVectorInput,
								   int[] featureColIdx, int vectorColIdx, DenseVector features) {
		if (isVectorInput) {
			Vector vec = VectorUtil.getVector(selection.get(vectorColIdx));
			if (null == vec) {
				Arrays.fill(features.getData(), 0.0);
			} else {
				if (vec instanceof DenseVector) {
					features.setData(((DenseVector) vec).getData());
				} else {
					Arrays.fill(features.getData(), 0.0);
					int[] indices = ((SparseVector) vec).getIndices();
					double[] vals = ((SparseVector) vec).getValues();
					for(int i = 0; i < indices.length; ++i) {
						features.set(indices[i], vals[i]);
					}
				}
			}
		} else {
			int n = featureColIdx.length;
			for (int i = 0; i < n; i++) {
				double v = ((Number) selection.get(i)).doubleValue();
				features.set(i, v);
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		MlpcModelData model = new MlpcModelDataConverter().load(modelRows);
		model.labelType = super.getModelSchema().getFieldTypes()[2];

		this.labels = model.labels;
		int[] layerSize0 = model.meta.get(MultilayerPerceptronTrainParams.LAYERS);
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
			featureColNames = model.meta.get(MultilayerPerceptronTrainParams.FEATURE_COLS);
			this.featureColIdx = TableUtil.findColIndicesWithAssert(dataSchema.getFieldNames(), featureColNames);
		}
		threadLocalVec =
			ThreadLocal.withInitial(() -> new DenseVector(layerSize0[0]));
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		DenseVector x = threadLocalVec.get();
		getFeaturesVector(selection, isVectorInput, featureColIdx, vectorColIdx, x);
		DenseVector prob = this.topo.predict(x);
		Map <Comparable<?>, Double> predDetailMap = new HashMap <>(prob.size());

		int argmax = -1;
		double maxProb = 0.;
		for (int i = 0; i < prob.size(); i++) {
			if (prob.get(i) > maxProb) {
				argmax = i;
				maxProb = prob.get(i);
			}
		}
		for (int i = 0; i < prob.size(); i++) {
			predDetailMap.put((Comparable<?>) labels.get(i), prob.get(i));
		}
		return Tuple2.of(labels.get(argmax), JsonConverter.toJson(predDetailMap));
	}


	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}
}
