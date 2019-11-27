package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sql.JoinBatchOp;
import com.alibaba.alink.operator.common.recommendation.AlsModelDataConverter;
import com.alibaba.alink.operator.common.recommendation.AlsPredict;
import com.alibaba.alink.params.recommendation.AlsPredictParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Make predictions based on model trained from AlsTrainBatchOp.
 * <p>
 * There are two types of predictions:
 * 1) rating prediction: given user and item, predict the rating.
 * 2) recommend prediction: given a list of users, recommend k items for each users.
 */
@SuppressWarnings("unchecked")
public final class AlsPredictBatchOp
    extends BatchOperator<AlsPredictBatchOp>
    implements AlsPredictParams<AlsPredictBatchOp> {

    public AlsPredictBatchOp() {
        this(new Params());
    }

    public AlsPredictBatchOp(Params params) {
        super(params);
    }

    private static DataSet<Tuple2<Long, float[]>> getFactors(BatchOperator<?> model, final int identity) {
        return model.getDataSet()
            .flatMap(new FlatMapFunction<Row, Tuple2<Long, float[]>>() {
                @Override
                public void flatMap(Row value, Collector<Tuple2<Long, float[]>> out) throws Exception {
                    int w = AlsModelDataConverter.getIsUser(value) ? 0 : 1;
                    if (w != identity) {
                        return;
                    }

                    long idx = AlsModelDataConverter.getVertexId(value);
                    float[] factors = AlsModelDataConverter.getFactors(value);
                    out.collect(Tuple2.of(idx, factors));
                }
            });
    }

    @Override
    public AlsPredictBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);

        BatchOperator model = inputs[0];
        BatchOperator data = inputs[1];

        this.setOutputTable(rate(model, data));
        return this;
    }

    /**
     * Recommend items for users.
     *
     * @param model The model trained from AlsTrainBatchOp.
     * @param data  The prediction data which contains users.
     * @return Recommended items for each users.
     */
//    public Table recommendForUsers(BatchOperator model, BatchOperator data) {
//        String userColName = getUserCol();
//        String predResultColName = getPredictionCol();
//        int topk = getTopK();
//
//        data = data.select("`" + userColName + "`");
//        DataSet<Tuple1<Long>> users = data.getDataSet()
//            .map(new MapFunction<Row, Tuple1<Long>>() {
//                @Override
//                public Tuple1<Long> map(Row value) throws Exception {
//                    return Tuple1.of(((Number) value.getField(0)).longValue());
//                }
//            });
//
//        DataSet<Tuple2<Long, float[]>> userFactors = getFactors(model, 0);
//        DataSet<Tuple2<Long, float[]>> itemFactors = getFactors(model, 1);
//
//        DataSet<Tuple2<Long, String>> recommend = AlsPredict.recommendForUsers(userFactors, itemFactors, users, topk);
//
//        DataSet<Row> output = recommend
//            .map(new MapFunction<Tuple2<Long, String>, Row>() {
//                @Override
//                public Row map(Tuple2<Long, String> value) throws Exception {
//                    return Row.of(value.f0, value.f1);
//                }
//            });
//        setOutput(output, new String[]{userColName, predResultColName},
//            new TypeInformation[]{Types.LONG, Types.STRING});
//
//        return this.getOutput();
//    }

    /**
     * Recommend users for items
     *
     * @param model The model trained from AlsTrainBatchOp.
     * @param data  The prediction data which contains items.
     * @return Recommended users for each items.
     */
//    public Table recommendForItems(BatchOperator model, BatchOperator data) {
//        String itemColName = getItemCol();
//        String predResultColName = getPredictionCol();
//        int topk = getTopK();
//
//        data = data.select("`" + itemColName + "`");
//        DataSet<Tuple1<Long>> items = data.getDataSet()
//            .map(new MapFunction<Row, Tuple1<Long>>() {
//                @Override
//                public Tuple1<Long> map(Row value) throws Exception {
//                    return Tuple1.of(((Number) value.getField(0)).longValue());
//                }
//            });
//
//        DataSet<Tuple2<Long, float[]>> userFactors = getFactors(model, 0);
//        DataSet<Tuple2<Long, float[]>> itemFactors = getFactors(model, 1);
//
//        DataSet<Tuple2<Long, String>> recommend = AlsPredict.recommendForUsers(itemFactors, userFactors, items, topk);
//
//        DataSet<Row> output = recommend
//            .map(new MapFunction<Tuple2<Long, String>, Row>() {
//                @Override
//                public Row map(Tuple2<Long, String> value) throws Exception {
//                    return Row.of(value.f0, value.f1);
//                }
//            });
//        setOutput(output, new String[]{itemColName, predResultColName},
//            new TypeInformation[]{Types.LONG, Types.STRING});
//
//        return this.getOutput();
//    }

    /**
     * Predict ratings give a user and an item.
     *
     * @param model The model trained from AlsTrainBatchOp.
     * @param data  The prediction data which contains user-item pairs.
     * @return The predicted rating given by the user to the item.
     */
    public Table rate(BatchOperator model, BatchOperator data) {
        String userColName = getUserCol();
        String itemColName = getItemCol();
        String predResultColName = getPredictionCol();
        final int userColIdx = TableUtil.findColIndex(data.getColNames(), userColName);
        final int itemColIdx = TableUtil.findColIndex(data.getColNames(), itemColName);

        DataSet<Tuple2<Long, float[]>> userFactors = getFactors(model, 0);
        DataSet<Tuple2<Long, float[]>> itemFactors = getFactors(model, 1);

        DataSet<Tuple2<Long, Long>> ui = data
            .getDataSet()
            .map(new MapFunction<Row, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> map(Row value) throws Exception {
                    return new Tuple2<>(((Number) value.getField(userColIdx)).longValue(),
                        ((Number) value.getField(itemColIdx)).longValue());
                }
            });

        DataSet<Tuple3<Long, Long, Double>> rating = AlsPredict.rate(userFactors, itemFactors, ui);

        DataSet<Row> output = rating
            .map(new MapFunction<Tuple3<Long, Long, Double>, Row>() {
                @Override
                public Row map(Tuple3<Long, Long, Double> value) throws Exception {
                    return Row.of(value.f0, value.f1, value.f2);
                }
            });

        Table ratingTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), output,
            new String[]{userColName, itemColName, predResultColName},
            new TypeInformation[]{Types.LONG, Types.LONG, Types.DOUBLE});

        return new JoinBatchOp(
            String.format("a.`%s`=b.`%s` and a.`%s`=b.`%s`", userColName, userColName, itemColName, itemColName),
            String.format("a.*,b.`%s`", predResultColName))
            .setMLEnvironmentId(getMLEnvironmentId())
            .linkFrom(data, BatchOperator.fromTable(ratingTable).setMLEnvironmentId(getMLEnvironmentId())).getOutputTable();
    }
}