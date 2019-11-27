package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * The ModelMapper for {@link com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel}.
 */
public class MlpcModelMapper extends RichModelMapper {

    private boolean isVectorInput;
    private int vectorColIdx;
    private int[] featureColIdx;
    private transient TopologyModel topo;
    private transient List<Object> labels;
    private transient Map<Comparable, Double> predDetailMap;

    public MlpcModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
    }

    public static DenseVector getFeaturesVector(Row row, boolean isVectorInput,
                                                int[] featureColIdx, int vectorColIdx) {
        if (isVectorInput) {
            Vector vec = VectorUtil.getVector(row.getField(vectorColIdx));
            if (null == vec) {
                return null;
            } else {
                return (vec instanceof DenseVector) ? (DenseVector) vec
                    : ((SparseVector) vec).toDenseVector();
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
    public void loadModel(List<Row> modelRows) {
        MlpcModelData model = new MlpcModelDataConverter().load(modelRows);
        model.labelType = super.getModelSchema().getFieldTypes()[2];

        this.labels = model.labels;
        int[] layerSize0 = model.meta.get(MultilayerPerceptronTrainParams.LAYERS);
        Topology topology = FeedForwardTopology.multiLayerPerceptron(layerSize0, true);
        this.topo = topology.getModel(model.weights);
        this.predDetailMap = new HashMap<>(layerSize0[layerSize0.length - 1]);
        isVectorInput = model.meta.get(ModelParamName.IS_VECTOR_INPUT);

        TableSchema dataSchema = getDataSchema();
        if (isVectorInput) {
            String vectorColName = params.contains(MultilayerPerceptronPredictParams.VECTOR_COL) ?
                params.get(MultilayerPerceptronPredictParams.VECTOR_COL) :
                model.meta.get(MultilayerPerceptronPredictParams.VECTOR_COL);
            this.vectorColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), vectorColName);
            assert this.vectorColIdx >= 0;
        } else {
            String[] featureColNames = model.meta.get(MultilayerPerceptronTrainParams.FEATURE_COLS);
            this.featureColIdx = TableUtil.findColIndices(dataSchema.getFieldNames(), featureColNames);
        }
    }

    @Override
    protected Object predictResult(Row row) throws Exception {
        return predictResultDetail(row).f0;
    }

    @Override
    protected Tuple2<Object, String> predictResultDetail(Row row) throws Exception {
        DenseVector x = getFeaturesVector(row, isVectorInput, featureColIdx, vectorColIdx);
        DenseVector prob = this.topo.predict(x);
        int argmax = -1;
        double maxProb = 0.;
        for (int i = 0; i < prob.size(); i++) {
            if (prob.get(i) > maxProb) {
                argmax = i;
                maxProb = prob.get(i);
            }
        }
        for (int i = 0; i < prob.size(); i++) {
            this.predDetailMap.put((Comparable) labels.get(i), prob.get(i));
        }
        return Tuple2.of(labels.get(argmax), gson.toJson(predDetailMap));
    }
}
