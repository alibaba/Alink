package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.operator.common.dataproc.BlockwiseCross;
import com.github.fommil.netlib.BLAS;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;

/**
 * Prediction utilities for {@link com.alibaba.alink.pipeline.recommendation.ALS}.
 */
public class AlsPredict {

    /**
     * Recommend for each user <code>topK</code> items. If some of the users have no factors,
     * then no recommendations will be generated for them.
     *
     * @param userFactors user factors
     * @param itemFactors item factors
     * @param users       a dataset of users to recommend items for
     * @param topK        number of items to recommend
     * @return a dataset of (user, item_list)
     */
    public static DataSet<Tuple2<Long, String>> recommendForUsers(
        DataSet<Tuple2<Long, float[]>> userFactors,
        DataSet<Tuple2<Long, float[]>> itemFactors,
        DataSet<Tuple1<Long>> users, final int topK) {

        if (users != null) {
            userFactors = userFactors.join(users)
                .where(0).equalTo(0)
                .projectFirst(0, 1);
        }

        DataSet<Tuple3<Long, long[], float[]>> rec = BlockwiseCross
            .findTopK(userFactors, itemFactors, topK, Order.DESCENDING,
                new BlockwiseCross.BulkScoreFunction<float[], float[]>() {
                    transient long[] ids;
                    transient float[] factors;
                    transient float[] buffer;
                    transient List<Tuple2<Long, Float>> scoreBuffer;
                    transient BLAS blas;

                    @Override
                    public void addTargets(Iterable<Tuple3<Integer, Long, float[]>> iterable) {
                        List<Tuple3<Integer, Long, float[]>> cache = new ArrayList<>();
                        iterable.forEach(cache::add);
                        int dim = 0;
                        if (cache.size() > 0) {
                            dim = cache.get(0).f2.length;
                        }
                        ids = new long[cache.size()];
                        factors = new float[cache.size() * dim];
                        scoreBuffer = new ArrayList<>(cache.size());
                        buffer = new float[cache.size()];
                        for (int i = 0; i < cache.size(); i++) {
                            ids[i] = cache.get(i).f1;
                            scoreBuffer.add(Tuple2.of(ids[i], 0.F));
                            System.arraycopy(cache.get(i).f2, 0, factors, i * dim, dim);
                        }
                        blas = com.github.fommil.netlib.BLAS.getInstance();
                    }

                    @Override
                    public List<Tuple2<Long, Float>> scoreAll(Long id1, float[] v1) {
                        int m = ids.length;
                        if (m == 0) {
                            return scoreBuffer;
                        }
                        int n = factors.length / m;
                        blas.sgemv("t", n, m, 1.F, factors, n, v1, 1, 0.F, buffer, 1);
                        for (int i = 0; i < m; i++) {
                            scoreBuffer.get(i).setFields(ids[i], buffer[i]);
                        }
                        return scoreBuffer;
                    }
                });

        return rec
            .map(new MapFunction<Tuple3<Long, long[], float[]>, Tuple2<Long, String>>() {
                @Override
                public Tuple2<Long, String> map(Tuple3<Long, long[], float[]> value) throws Exception {
                    StringBuilder sbd = new StringBuilder();
                    int n = value.f1.length;

                    for (int i = 0; i < n; i++) {
                        if (i > 0) {
                            sbd.append(",");
                        }
                        sbd.append(value.f1[i]).append(":").append(value.f2[i]);
                    }
                    return Tuple2.of(value.f0, sbd.toString());
                }
            });
    }

    /**
     * Predict ratings given each user-item pairs. If factors of user or item is missing,
     * then predict null value for them.
     *
     * @param userFactors user factors
     * @param itemFactors item factors
     * @param input       a dataset of (user, item) tuples
     * @return a dataset of (user, item, rating) tuples
     */
    public static DataSet<Tuple3<Long, Long, Double>> rate(
        DataSet<Tuple2<Long, float[]>> userFactors,
        DataSet<Tuple2<Long, float[]>> itemFactors,
        DataSet<Tuple2<Long, Long>> input) {

        return input.leftOuterJoin(userFactors)
            .where(0).equalTo(0)
            .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, float[]>, Tuple3<Long, Long, float[]>>() {
                @Override
                public Tuple3<Long, Long, float[]> join(Tuple2<Long, Long> first, Tuple2<Long, float[]> second) throws Exception {
                    if (second != null) {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    } else {
                        return Tuple3.of(first.f0, first.f1, new float[0]);
                    }
                }
            })
            .leftOuterJoin(itemFactors)
            .where(1).equalTo(0)
            .with(new JoinFunction<Tuple3<Long, Long, float[]>, Tuple2<Long, float[]>, Tuple4<Long, Long, float[], float[]>>() {
                @Override
                public Tuple4<Long, Long, float[], float[]> join(Tuple3<Long, Long, float[]> first, Tuple2<Long, float[]> second) throws Exception {
                    if (second != null) {
                        return Tuple4.of(first.f0, first.f1, first.f2, second.f1);
                    } else {
                        return Tuple4.of(first.f0, first.f1, first.f2, new float[0]);
                    }
                }
            })
            .map(new MapFunction<Tuple4<Long, Long, float[], float[]>, Tuple3<Long, Long, Double>>() {
                @Override
                public Tuple3<Long, Long, Double> map(Tuple4<Long, Long, float[], float[]> value) throws Exception {
                    if (value.f2.length > 0 && value.f3.length > 0) {
                        double s = 0.;
                        for (int i = 0; i < value.f2.length; i++) {
                            s += value.f2[i] * value.f3[i];
                        }
                        return Tuple3.of(value.f0, value.f1, s);
                    } else {
                        return Tuple3.of(value.f0, value.f1, null);
                    }
                }
            });
    }
}

