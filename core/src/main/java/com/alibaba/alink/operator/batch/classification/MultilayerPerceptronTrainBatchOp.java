package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.ann.*;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MultilayerPerceptronClassifier is a neural network based multi-class classifier.
 * Valina neural network with all dense layers are used, the output layer is a softmax layer.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 */
public final class MultilayerPerceptronTrainBatchOp
    extends BatchOperator<MultilayerPerceptronTrainBatchOp>
    implements MultilayerPerceptronTrainParams<MultilayerPerceptronTrainBatchOp> {

    public MultilayerPerceptronTrainBatchOp() {
        this(new Params());
    }

    public MultilayerPerceptronTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * Get distinct labels and assign each label an index.
     */
    private static DataSet<Tuple2<Long, Object>> getDistinctLabels(BatchOperator data, final String labelColName) {
        data = data.select("`" + labelColName + "`").distinct();
        DataSet<Row> labelRows = data.getDataSet();
        return DataSetUtils.zipWithIndex(labelRows)
            .map(new MapFunction<Tuple2<Long, Row>, Tuple2<Long, Object>>() {
                @Override
                public Tuple2<Long, Object> map(Tuple2<Long, Row> value) throws Exception {
                    return Tuple2.of(value.f0, value.f1.getField(0));
                }
            })
            .name("get_labels");
    }

    /**
     * Get training samples from input data.
     */
    private static DataSet<Tuple2<Double, DenseVector>> getTrainingSamples(
        BatchOperator data, DataSet<Tuple2<Long, Object>> labels,
        final String[] featureColNames, final String vectorColName, final String labelColName) {

        final boolean isVectorInput = !StringUtils.isNullOrWhitespaceOnly(vectorColName);
        final int vectorColIdx = isVectorInput ? TableUtil.findColIndex(data.getColNames(), vectorColName) : -1;
        final int[] featureColIdx = isVectorInput ? null : TableUtil.findColIndices(data.getSchema(),
            featureColNames);
        final int labelColIdx = TableUtil.findColIndex(data.getColNames(), labelColName);

        DataSet<Row> dataRows = data.getDataSet();
        return dataRows
            .map(new RichMapFunction<Row, Tuple2<Double, DenseVector>>() {
                transient Map<Comparable, Long> label2index;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Tuple2<Long, Object>> bcLabels = getRuntimeContext().getBroadcastVariable("labels");
                    this.label2index = new HashMap<>();
                    bcLabels.forEach(t2 -> {
                        Long index = t2.f0;
                        Comparable label = (Comparable) t2.f1;
                        this.label2index.put(label, index);
                    });
                }

                @Override
                public Tuple2<Double, DenseVector> map(Row value) throws Exception {
                    Comparable label = (Comparable) value.getField(labelColIdx);
                    Long labelIdx = this.label2index.get(label);
                    if (labelIdx == null) {
                        throw new RuntimeException("unknown label: " + label);
                    }
                    if (isVectorInput) {
                        Vector vec = VectorUtil.getVector(value.getField(vectorColIdx));
                        if (null == vec) {
                            return new Tuple2<>(labelIdx.doubleValue(), null);
                        } else {
                            return new Tuple2<>(labelIdx.doubleValue(),
                                (vec instanceof DenseVector) ? (DenseVector) vec
                                    : ((SparseVector) vec).toDenseVector());
                        }
                    } else {
                        int n = featureColIdx.length;
                        DenseVector features = new DenseVector(n);
                        for (int i = 0; i < n; i++) {
                            double v = ((Number) value.getField(featureColIdx[i])).doubleValue();
                            features.set(i, v);
                        }
                        return Tuple2.of(labelIdx.doubleValue(), features);
                    }
                }
            })
            .withBroadcastSet(labels, "labels");
    }

    @Override
    public MultilayerPerceptronTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        final String labelColName = getLabelCol();
        final String vectorColName = getVectorCol();
        final boolean isVectorInput = !StringUtils.isNullOrWhitespaceOnly(vectorColName);
        final String[] featureColNames = isVectorInput ? null :
            (getParams().contains(FEATURE_COLS) ? getFeatureCols() :
                TableUtil.getNumericCols(in.getSchema(), new String[]{labelColName}));

        final TypeInformation<?> labelType = in.getColTypes()[TableUtil.findColIndex(in.getColNames(),
            labelColName)];
        DataSet<Tuple2<Long, Object>> labels = getDistinctLabels(in, labelColName);

        // get train data
        DataSet<Tuple2<Double, DenseVector>> trainData =
            getTrainingSamples(in, labels, featureColNames, vectorColName, labelColName);

        // train
        final int[] layerSize = getLayers();
        final int blockSize = getBlockSize();
        final DenseVector initialWeights = getInitialWeights();
        Topology topology = FeedForwardTopology.multiLayerPerceptron(layerSize, true);
        FeedForwardTrainer trainer = new FeedForwardTrainer(topology,
            layerSize[0], layerSize[layerSize.length - 1], true, blockSize, initialWeights);
        DataSet<DenseVector> weights = trainer.train(trainData, getParams());

        // output model
        DataSet<Row> modelRows = weights
            .flatMap(new RichFlatMapFunction<DenseVector, Row>() {
                @Override
                public void flatMap(DenseVector value, Collector<Row> out) throws Exception {
                    List<Tuple2<Long, Object>> bcLabels = getRuntimeContext().getBroadcastVariable("labels");
                    Object[] labels = new Object[bcLabels.size()];
                    bcLabels.forEach(t2 -> {
                        labels[t2.f0.intValue()] = t2.f1;
                    });

                    MlpcModelData model = new MlpcModelData(labelType);
                    model.labels = Arrays.asList(labels);
                    model.meta.set(ModelParamName.IS_VECTOR_INPUT, isVectorInput);
                    model.meta.set(MultilayerPerceptronTrainParams.LAYERS, layerSize);
                    model.meta.set(MultilayerPerceptronTrainParams.VECTOR_COL, vectorColName);
                    model.meta.set(MultilayerPerceptronTrainParams.FEATURE_COLS, featureColNames);
                    model.weights = value;
                    new MlpcModelDataConverter(labelType).save(model, out);
                }
            })
            .withBroadcastSet(labels, "labels");

        setOutput(modelRows, new MlpcModelDataConverter(labelType).getModelSchema());
        return this;
    }
}
