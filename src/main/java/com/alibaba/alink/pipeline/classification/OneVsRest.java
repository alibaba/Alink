package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.params.classification.OneVsRestTrainParams;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k models (one per class).
 * Each example is scored against all k models and the model with highest score
 * is picked to label the example.
 */
public class OneVsRest extends EstimatorBase<OneVsRest, OneVsRestModel>
    implements OneVsRestTrainParams<OneVsRest>, OneVsRestPredictParams<OneVsRest> {
    private EstimatorBase classifier;

    private static BatchOperator generateTrainData(BatchOperator data, BatchOperator allLabels, BatchOperator
        prevModel, final int iLabel, final int labelColIdx) {

        DataSet<Integer> barrier;
        if (prevModel != null) {
            barrier = ((DataSet<Row>) prevModel.getDataSet())
                .mapPartition(new MapPartitionFunction<Row, Integer>() {
                    @Override
                    public void mapPartition(Iterable<Row> values, Collector<Integer> out) throws Exception {
                    }
                });
        } else {
            barrier = MLEnvironmentFactory.get(data.getMLEnvironmentId()).getExecutionEnvironment().fromElements(0);
        }

        DataSet<Row> dataSet = data.getDataSet();
        dataSet = dataSet
            .map(new RichMapFunction<Row, Row>() {
                transient Object label;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Row> bc = getRuntimeContext().getBroadcastVariable("allLabels");
                    Integer[] order = new Integer[bc.size()];
                    for (int i = 0; i < order.length; i++) {
                        order[i] = i;
                    }
                    Arrays.sort(order, new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            Comparable v1 = (Comparable) bc.get(o1).getField(0);
                            Comparable v2 = (Comparable) bc.get(o2).getField(0);
                            return v1.compareTo(v2);
                        }
                    });
                    if (iLabel >= bc.size()) {
                        throw new RuntimeException(
                            "the specified numClasses is larger than the number of distinct labels.: " +
                                String.format("iLabel = %d, num lables = %d", iLabel, bc.size()));
                    }
                    this.label = bc.get(order[iLabel]).getField(0);
                }

                @Override
                public Row map(Row value) throws Exception {
                    for (int i = 0; i < value.getArity(); i++) {
                        if (i == labelColIdx) {
                            if (value.getField(i).equals(label)) {
                                value.setField(i, 1.0);
                            } else {
                                value.setField(i, 0.0);
                            }
                        }
                    }
                    return value;
                }
            })
            //                .withBroadcastSet(barrier, "barrier")
            .withBroadcastSet(allLabels.getDataSet(), "allLabels")
            .name("CreateTrainData#" + iLabel);

        TypeInformation[] colTypes = data.getColTypes().clone();
        colTypes[labelColIdx] = Types.DOUBLE;
        return new DataSetWrapperBatchOp(dataSet, data.getColNames(), colTypes)
            .setMLEnvironmentId(data.getMLEnvironmentId());
    }

    public OneVsRest setClassifier(EstimatorBase classifier) {
        this.classifier = classifier;
        return this;
    }

    @Override
    public OneVsRestModel fit(BatchOperator input) {
        BatchOperator data = input;
        String labelColName = classifier.getParams().get(HasLabelCol.LABEL_COL);
        BatchOperator allLabels = getAllLabels(data, labelColName);
        int numClasses = getNumClass();

        int labelColIdx = TableUtil.findColIndex(data.getColNames(), labelColName);
        assert (labelColIdx >= 0);
        TypeInformation labelColType = data.getColTypes()[labelColIdx];

        ModelBase[] models = new ModelBase[numClasses];
        for (int iCls = 0; iCls < numClasses; iCls++) {
            this.classifier.set(HasPositiveLabelValueString.POS_LABEL_VAL_STR, "1");
            BatchOperator prevModel = iCls == 0 ? null : new TableSourceBatchOp(
                ((ModelBase) models[iCls - 1]).getModelData())
                .setMLEnvironmentId(input.getMLEnvironmentId());
            BatchOperator trainData = generateTrainData(data, allLabels, prevModel, iCls, labelColIdx);
            models[iCls] = this.classifier.fit(trainData);
        }

        Table modelData = unionAllModels(models);

        Params meta = new Params()
            .set(ModelParamName.NUM_CLASSES, numClasses)
            .set(ModelParamName.BIN_CLS_CLASS_NAME, this.classifier.getClass().getCanonicalName())
            .set(ModelParamName.BIN_CLS_PARAMS, this.classifier.getParams().toJson())
            .set(ModelParamName.LABEL_TYPE_NAME, FlinkTypeConverter.getTypeString(labelColType))
            .set(ModelParamName.MODEL_COL_NAMES, ((ModelBase) models[0]).getModelData().getSchema().getFieldNames())
            .set(ModelParamName.MODEL_COL_TYPES,
                toJdbcColTypes(((ModelBase) models[0]).getModelData().getSchema().getFieldTypes()));

        Table modelMeta = createModelMeta(meta, allLabels, labelColType);

        OneVsRestModel oneVsRestModel = new OneVsRestModel(classifier.getParams().clone().merge(this.getParams()));
        oneVsRestModel.setModelData(TableUtil.concatTables(new Table[]{modelMeta, modelData, allLabels.getOutputTable()}, getMLEnvironmentId()));
        return oneVsRestModel;
    }

    private Table createModelMeta(final Params meta, BatchOperator allLabels, TypeInformation labelColType) {
        DataSet<Row> metaDataSet = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(meta.toJson())
            .map(new RichMapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    Params params = Params.fromJson(value);
                    List<Row> bc = getRuntimeContext().getBroadcastVariable("allLabels");
                    Integer[] order = new Integer[bc.size()];
                    for (int i = 0; i < order.length; i++) {
                        order[i] = i;
                    }
                    Arrays.sort(order, new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            Comparable v1 = (Comparable) bc.get(o1).getField(0);
                            Comparable v2 = (Comparable) bc.get(o2).getField(0);
                            return v1.compareTo(v2);
                        }
                    });
                    List<Object> orderedLabels = new ArrayList<>(order.length);
                    for (int i = 0; i < order.length; i++) {
                        orderedLabels.add(bc.get(order[i]).getField(0));
                    }
                    params.set(ModelParamName.LABELS, gson.toJson(orderedLabels, ArrayList.class));
                    return params.toJson();
                }
            })
            .withBroadcastSet(allLabels.getDataSet(), "allLabels")
            .map(new MapFunction<String, Row>() {
                @Override
                public Row map(String value) throws Exception {
                    return Row.of(value);
                }
            });
        return DataSetConversionUtil.toTable(getMLEnvironmentId(), metaDataSet, new String[]{"meta"}, new TypeInformation[]{Types.STRING});
    }

    private Table unionAllModels(ModelBase[] models) {
        BatchOperator allModel = null;
        for (int i = 0; i < models.length; i++) {
            BatchOperator model = BatchOperator.fromTable(models[i].getModelData())
                .setMLEnvironmentId(getMLEnvironmentId());
            model = model.select(String.format("CAST(%d as bigint) AS ovr_id, *", i));
            if (i == 0) {
                allModel = model;
            } else {
                allModel = new UnionAllBatchOp()
                    .setMLEnvironmentId(getMLEnvironmentId())
                    .linkFrom(allModel, model);
            }
        }
        return allModel.getOutputTable();
    }

    private Integer[] toJdbcColTypes(TypeInformation[] colTypes) {
        Integer[] typesInInt = new Integer[colTypes.length];
        for (int i = 0; i < colTypes.length; i++) {
            typesInInt[i] = JdbcTypeConverter.getIntegerSqlType(colTypes[i]);
        }
        return typesInInt;
    }

    private BatchOperator getAllLabels(BatchOperator data, String labelColName) {
        final int labelColIdx = TableUtil.findColIndex(data.getColNames(), labelColName);
        assert labelColIdx >= 0;
        DataSet<Row> input = data.getDataSet();
        DataSet<Row> output = input
            .<Tuple1<Comparable>>mapPartition(new MapPartitionFunction<Row, Tuple1<Comparable>>() {
                @Override
                public void mapPartition(Iterable<Row> values, Collector<Tuple1<Comparable>> out) throws Exception {
                    Set<Object> distinctLabels = new HashSet<>();
                    values.forEach(row -> {
                        distinctLabels.add(row.getField(labelColIdx));
                    });
                    distinctLabels.forEach(l -> {
                        out.collect(Tuple1.of((Comparable) l));
                    });
                }
            })
            .groupBy(0)
            .reduceGroup(new GroupReduceFunction<Tuple1<Comparable>, Row>() {
                @Override
                public void reduce(Iterable<Tuple1<Comparable>> values, Collector<Row> out) throws Exception {
                    out.collect(Row.of(values.iterator().next().f0));
                }
            });
        return new DataSetWrapperBatchOp(output, new String[]{labelColName},
            new TypeInformation[]{data.getColTypes()[labelColIdx]})
            .setMLEnvironmentId(data.getMLEnvironmentId());
    }
}
