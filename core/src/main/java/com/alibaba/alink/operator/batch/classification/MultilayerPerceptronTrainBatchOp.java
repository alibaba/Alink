package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
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

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.ann.FeedForwardTopology;
import com.alibaba.alink.operator.common.classification.ann.FeedForwardTrainer;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelData;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelDataConverter;
import com.alibaba.alink.operator.common.classification.ann.Topology;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;
import com.alibaba.alink.params.shared.colname.HasFeatureCols;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

import java.util.ArrayList;
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
@InputPorts(values = {
    @PortSpec(PortType.DATA),
    @PortSpec(value = PortType.MODEL, isOptional = true)
})
@OutputPorts(values = {
    @PortSpec(PortType.MODEL)
})
@ParamSelectColumnSpec(name = "featureCols",
    allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
    allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@FeatureColsVectorColMutexRule

@NameCn("多层感知机分类训练")
public final class MultilayerPerceptronTrainBatchOp
        extends BatchOperator<MultilayerPerceptronTrainBatchOp>
        implements MultilayerPerceptronTrainParams<MultilayerPerceptronTrainBatchOp> {

    private static final long serialVersionUID = -1006049713058836208L;

    public MultilayerPerceptronTrainBatchOp() {
        this(new Params());
    }

    public MultilayerPerceptronTrainBatchOp(Params params) {
        super(params);
    }

    /**
     * Get distinct labels and assign each label an index.
     */
    private static DataSet<Tuple2<Long, Object>> getDistinctLabels(BatchOperator <?> data, final String labelColName) {
        data = data.select("`" + labelColName + "`").distinct();
        DataSet<Row> labelRows = data.getDataSet();
        return DataSetUtils.zipWithIndex(labelRows)
                .map(new MapFunction<Tuple2<Long, Row>, Tuple2<Long, Object>>() {
                    private static final long serialVersionUID = 6650168043579663372L;

                    @Override
                    public Tuple2<Long, Object> map(Tuple2<Long, Row> value) {
                        return Tuple2.of(value.f0, value.f1.getField(0));
                    }
                })
                .name("get_labels");
    }

    /**
     * Get distinct labels and assign each label an index.
     */
    private static DataSet<DenseVector> getMaxAbsVector(BatchOperator<?> data,
                                                        final String[] featureColNames,
                                                        final String vectorColName,
                                                        final int vecSize) {

        final boolean isVectorInput = !StringUtils.isNullOrWhitespaceOnly(vectorColName);
        final int vectorColIdx = isVectorInput ?
                TableUtil.findColIndexWithAssertAndHint(data.getColNames(), vectorColName) : -1;
        final int[] featureColIdx = isVectorInput ?
                null : TableUtil.findColIndicesWithAssertAndHint(data.getSchema(), featureColNames);
        return data.getDataSet().mapPartition(new MapPartitionFunction<Row, DenseVector>() {
            private static final long serialVersionUID = 7200866630508717163L;

            @Override
            public void mapPartition(Iterable<Row> iterable, Collector<DenseVector> collector) {
                DenseVector maxAbs = null;
                if (isVectorInput) {
                    for (Row value : iterable) {
                        Vector vec = VectorUtil.getVector(value.getField(vectorColIdx));
                        if (maxAbs == null) {
                            maxAbs = new DenseVector(vecSize);
                            if (vec instanceof DenseVector) {
                                for (int i = 0; i < vec.size(); ++i) {
                                    maxAbs.set(i, Math.abs(vec.get(i)));
                                }
                            } else {
                                int[] indices = ((SparseVector) vec).getIndices();
                                for (int index : indices) {
                                    maxAbs.set(index, Math.abs(vec.get(index)));
                                }
                            }
                        } else {
                            if (vec instanceof DenseVector) {
                                for (int i = 0; i < maxAbs.size(); ++i) {
                                    maxAbs.set(i, Math.max(maxAbs.get(i), Math.abs(vec.get(i))));
                                }
                            } else {
                                int[] indices = ((SparseVector) vec).getIndices();
                                for (int index : indices) {
                                    maxAbs.set(index,
                                        Math.max(maxAbs.get(index), Math.abs(vec.get(index))));
                                }
                            }
                        }
                    }
                } else {
                    int n = featureColIdx.length;
                    for (Row value : iterable) {
                        if (maxAbs == null) {
                            maxAbs = new DenseVector(n);
                            for (int i = 0; i < n; i++) {
                                double v = ((Number) value.getField(featureColIdx[i])).doubleValue();
                                maxAbs.set(i, Math.abs(v));
                            }
                        } else {
                            for (int i = 0; i < n; i++) {
                                double v = ((Number) value.getField(featureColIdx[i])).doubleValue();
                                maxAbs.set(i, Math.max(maxAbs.get(i), Math.abs(v)));
                            }
                        }
                    }
                }

                if (maxAbs == null) {
                    // If collect null value: "Caused by: java.lang.NullPointerException: The system does not support
                    // records that are null.Null values are only supported as fields inside other objects." will be
                    // thrown.

                    return;
                }
                collector.collect(maxAbs);
            }
        }).reduceGroup(new GroupReduceFunction<DenseVector, DenseVector>() {
            private static final long serialVersionUID = 880634306611878638L;

            @Override
            public void reduce(Iterable<DenseVector> iterable, Collector<DenseVector> collector) {
                DenseVector maxAbs = null;
                for (DenseVector vec : iterable) {
                    if (maxAbs == null) {
                        maxAbs = vec;
                    } else {
                        for (int i = 0; i < maxAbs.size(); ++i) {
                            maxAbs.set(i, Math.max(maxAbs.get(i), Math.abs(vec.get(i))));
                        }
                    }
                }
                collector.collect(maxAbs);
            }
        });
    }

    /**
     * Get training samples from input data.
     */
    private static DataSet<Tuple2<Double, DenseVector>> getTrainingSamples(
            BatchOperator <?> data, DataSet<Tuple2<Long, Object>> labels, DataSet<DenseVector> maxAbs,
            final String[] featureColNames, final String vectorColName, final String labelColName, final int vecSize) {

        final boolean isVectorInput = !StringUtils.isNullOrWhitespaceOnly(vectorColName);
        final int vectorColIdx = isVectorInput ? TableUtil.findColIndexWithAssertAndHint(data.getColNames(),
                vectorColName) : -1;
        final int[] featureColIdx = isVectorInput ? null : TableUtil.findColIndicesWithAssertAndHint(data.getSchema(),
                featureColNames);
        final int labelColIdx = TableUtil.findColIndexWithAssertAndHint(data.getColNames(), labelColName);

        DataSet<Row> dataRows = data.getDataSet();
        return dataRows
                .map(new RichMapFunction<Row, Tuple2<Double, DenseVector>>() {
                    private static final long serialVersionUID = -2883936655064900395L;
                    transient Map<Comparable<?>, Long> label2index;
                    private DenseVector maxAbs;

                    @Override
                    public void open(Configuration parameters) {
                        List<Tuple2<Long, Object>> bcLabels = getRuntimeContext().getBroadcastVariable("labels");
                        this.label2index = new HashMap<>();
                        bcLabels.forEach(t2 -> {
                            Long index = t2.f0;
                            Comparable<?> label = (Comparable<?>) t2.f1;
                            this.label2index.put(label, index);
                        });
                        maxAbs = (DenseVector) getRuntimeContext().getBroadcastVariable("maxAbs").get(0);
                        for (int i = 0; i < maxAbs.size(); ++i) {
                            if (maxAbs.get(i) == 0) {
								maxAbs.set(i, 1.0);
							}
                        }
                    }

                    @Override
                    public Tuple2<Double, DenseVector> map(Row value) {
                        Comparable<?> label = (Comparable<?>) value.getField(labelColIdx);
                        Long labelIdx = this.label2index.get(label);
                        if (labelIdx == null) {
                            throw new RuntimeException("unknown label: " + label);
                        }
                        if (isVectorInput) {
                            Vector vec = VectorUtil.getVector(value.getField(vectorColIdx));
                            DenseVector finalVec;
                            if (null == vec) {
                                return new Tuple2<>(labelIdx.doubleValue(), null);
                            } else {
                                if (vec instanceof DenseVector) {
                                    finalVec = (DenseVector) vec;
                                    for (int i = 0; i < maxAbs.size(); ++i) {
                                        finalVec.set(i, finalVec.get(i) / maxAbs.get(i));
                                    }
                                } else {
                                    SparseVector tmpVec = (SparseVector) vec;
                                    tmpVec.setSize(vecSize);
                                    finalVec = tmpVec.toDenseVector();
                                    int[] indices = ((SparseVector) vec).getIndices();
									for (int index : indices) {
										finalVec.set(index, finalVec.get(index) / maxAbs.get(index));
									}
                                }
                            }
                            return new Tuple2<>(labelIdx.doubleValue(), finalVec);
                        } else {
                            int n = featureColIdx.length;
                            DenseVector features = new DenseVector(n);
                            for (int i = 0; i < n; i++) {
                                double v = ((Number) value.getField(featureColIdx[i])).doubleValue();
                                features.set(i, v / maxAbs.get(i));
                            }
                            return Tuple2.of(labelIdx.doubleValue(), features);
                        }
                    }
                })
                .withBroadcastSet(labels, "labels")
                .withBroadcastSet(maxAbs, "maxAbs");
    }

    @Override
    public MultilayerPerceptronTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator <?> in;
        BatchOperator <?> initModel = null;
        if (inputs.length == 1) {
            in = checkAndGetFirst(inputs);
        } else {
            in = inputs[0];
            initModel = inputs[1];
        }

        final String labelColName = getLabelCol();
        final String vectorColName = getVectorCol();
        final boolean isVectorInput = !StringUtils.isNullOrWhitespaceOnly(vectorColName);
        if (getParams().contains(HasFeatureCols.FEATURE_COLS) && getParams().contains(HasVectorCol.VECTOR_COL)) {
            throw new RuntimeException("featureCols and vectorCol cannot be set at the same time.");
        }
        final String[] featureColNames = isVectorInput ? null :
                (getParams().contains(FEATURE_COLS) ? getFeatureCols() :
                        TableUtil.getNumericCols(in.getSchema(), new String[]{labelColName}));
        final TypeInformation<?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(),
                labelColName);
        DataSet<Tuple2<Long, Object>> labels = getDistinctLabels(in, labelColName);
        int vecSize = getLayers()[0];
        DataSet<DenseVector> maxAbs = getMaxAbsVector(in, featureColNames, vectorColName, vecSize);
        // get train data
        DataSet<Tuple2<Double, DenseVector>> trainData =
                getTrainingSamples(in, labels, maxAbs, featureColNames, vectorColName, labelColName, vecSize);

        // train
        final int[] layerSize = getLayers();
        final int blockSize = getBlockSize();
        final DenseVector initialWeights = getInitialWeights();
        Topology topology = FeedForwardTopology.multiLayerPerceptron(layerSize, true);
        FeedForwardTrainer trainer = new FeedForwardTrainer(topology,
                layerSize[0], layerSize[layerSize.length - 1], true, blockSize, initialWeights);
        DataSet<DenseVector> initialModel = initModel == null ? null : initModel.getDataSet().reduceGroup(
            new RichGroupReduceFunction <Row, DenseVector>() {
                @Override
                public void reduce(Iterable <Row> values, Collector <DenseVector> out) {
                    DenseVector maxAbs = (DenseVector) getRuntimeContext().getBroadcastVariable("maxAbs").get(0);
                    List <Row> modelRows = new ArrayList <>(0);
                    for (Row row : values) {
                        modelRows.add(row);
                    }
                    MlpcModelData model = new MlpcModelDataConverter().load(modelRows);

                    int[] modelLayerSize = model.meta.get(MultilayerPerceptronTrainParams.LAYERS);
					boolean judge = (model.meta.get(ModelParamName.IS_VECTOR_INPUT) == isVectorInput);
					if (!judge) {
						throw new RuntimeException("initial mlpc model not compatible with parameter setting: initial model need vector data.");
					}
					for (int i = 0; i < layerSize.length; ++i) {
						judge = (modelLayerSize[i] == layerSize[i]);
						if (!judge) {
							throw new RuntimeException("initial mlpc model not compatible with parameter setting. layerSize not equal.");
						}
					}

                    for (int i = 0; i < layerSize[0]; ++i) {
                        for (int j = 0; j < layerSize[1]; ++j) {
                            if (maxAbs.get(i) > 0) {
                                model.weights.set(layerSize[1] * i + j,
                                    model.weights.get(layerSize[1] * i + j) * maxAbs.get(i));
                            }
                        }
                    }
                    out.collect(model.weights);
                }
            }).withBroadcastSet(maxAbs, "maxAbs");
        DataSet<DenseVector> weights = trainer.train(trainData, initialModel, getParams());

        // output model
        DataSet<Row> modelRows = weights
                .flatMap(new RichFlatMapFunction<DenseVector, Row>() {
                    private static final long serialVersionUID = 9083288793177120814L;

                    @Override
                    public void flatMap(DenseVector value, Collector<Row> out) {
                        List<Tuple2<Long, Object>> bcLabels = getRuntimeContext().getBroadcastVariable("labels");
                        DenseVector maxAbs = (DenseVector) getRuntimeContext().getBroadcastVariable("maxAbs").get(0);
                        Object[] labels = new Object[bcLabels.size()];
                        bcLabels.forEach(t2 -> labels[t2.f0.intValue()] = t2.f1);
                        for (int i = 0; i < layerSize[0]; ++i) {
                            for (int j = 0; j < layerSize[1]; ++j) {
                                if (maxAbs.get(i) > 0) {
                                    value.set(layerSize[1] * i + j, value.get(layerSize[1] * i + j) / maxAbs.get(i));
                                }
                            }
                        }
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
                .withBroadcastSet(labels, "labels")
                .withBroadcastSet(maxAbs, "maxAbs");

        setOutput(modelRows, new MlpcModelDataConverter(labelType).getModelSchema());
        return this;
    }
}
