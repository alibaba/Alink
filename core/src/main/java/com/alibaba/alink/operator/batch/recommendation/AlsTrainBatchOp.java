package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.AlsModelDataConverter;
import com.alibaba.alink.operator.common.recommendation.AlsTrain;
import com.alibaba.alink.params.recommendation.AlsTrainParams;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Matrix factorization using Alternating Least Square method.
 * <p>
 * ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices.
 * Matrix R is usually a sparse matrix representing ratings given from users to items.
 * ALS tries to find X and Y that minimize || R - X * Yt ||^2. This is done by iterations.
 * At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 * <p>
 * The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 * <p>
 * We also support implicit preference model described in
 * "Collaborative Filtering for Implicit Feedback Datasets, 2008"
 */
public final class AlsTrainBatchOp
    extends BatchOperator<AlsTrainBatchOp>
    implements AlsTrainParams<AlsTrainBatchOp> {

    public AlsTrainBatchOp() {
        this(new Params());
    }

    public AlsTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public AlsTrainBatchOp linkFrom(List<BatchOperator<?>> ins) {
        return linkFrom(ins.get(0));
    }

    /**
     * Matrix decomposition using ALS algorithm.
     *
     * @param inputs a dataset of user-item-rating tuples
     * @return user factors and item factors.
     */
    @Override
    public AlsTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        final String userColName = getUserCol();
        final String itemColName = getItemCol();
        final String rateColName = getRateCol();

        final double lambda = getLambda();
        final int rank = getRank();
        final int numIter = getNumIter();
        final boolean nonNegative = getNonnegative();
        final boolean implicitPrefs = getImplicitPrefs();
        final double alpha = getAlpha();
        final int numMiniBatches = getNumBlocks();

        final int userColIdx = TableUtil.findColIndex(in.getColNames(), userColName);
        final int itemColIdx = TableUtil.findColIndex(in.getColNames(), itemColName);
        final int rateColIdx = TableUtil.findColIndex(in.getColNames(), rateColName);

        // tuple3: userId, itemId, rating
        DataSet<Tuple3<Long, Long, Float>> alsInput = in.getDataSet()
            .map(new MapFunction<Row, Tuple3<Long, Long, Float>>() {
                @Override
                public Tuple3<Long, Long, Float> map(Row value) {
                    return new Tuple3<>(((Number) value.getField(userColIdx)).longValue(),
                        ((Number) value.getField(itemColIdx)).longValue(),
                        ((Number) value.getField(rateColIdx)).floatValue());
                }
            });

        AlsTrain als = new AlsTrain(rank, numIter, lambda, implicitPrefs, alpha, numMiniBatches, nonNegative);
        DataSet<Tuple3<Byte, Long, float[]>> factors = als.fit(alsInput);

        DataSet<Row> output = factors.mapPartition(new RichMapPartitionFunction<Tuple3<Byte, Long, float[]>, Row>() {
            @Override
            public void mapPartition(Iterable<Tuple3<Byte, Long, float[]>> values, Collector<Row> out) {
                new AlsModelDataConverter(userColName, itemColName).save(values, out);
            }
        });

        this.setOutput(output, new AlsModelDataConverter(userColName, itemColName).getModelSchema());
        return this;
    }
}
