package com.alibaba.alink.common.utils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.dataproc.BlockwiseCross;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BlockwiseCrossTest {
    @Test
    public void testFindTopK() throws Exception {

        List<Tuple2<Long, float[]>> list1 = new ArrayList<>();
        list1.add(Tuple2.of(0L, new float[]{-1, 0}));
        list1.add(Tuple2.of(1L, new float[]{2, 0}));

        List<Tuple2<Long, float[]>> list2 = new ArrayList<>();
        list2.add(Tuple2.of(0L, new float[]{0, 0}));
        list2.add(Tuple2.of(1L, new float[]{1, 0}));
        list2.add(Tuple2.of(2L, new float[]{1, 1}));
        list2.add(Tuple2.of(3L, new float[]{0, 1}));

        ExecutionEnvironment env = MLEnvironmentFactory.getDefault().getExecutionEnvironment();
        env.startNewSession();
        DataSet<Tuple2<Long, float[]>> dataset1 = env.fromCollection(list1).rebalance();
        DataSet<Tuple2<Long, float[]>> dataset2 = env.fromCollection(list2).rebalance();

        DataSet<Tuple3<Long, long[], float[]>> topkDesc = BlockwiseCross
            .findTopK(dataset1, dataset2, 2, Order.DESCENDING,
                new BlockwiseCross.ScoreFunction<float[], float[]>() {
                    @Override
                    public float score(Long id1, float[] v1, Long id2, float[] v2) {
                        float s = 0.F;
                        for (int i = 0; i < v1.length; i++) {
                            s += (v1[i] - v2[i]) * (v1[i] - v2[i]);
                        }
                        return (float) Math.sqrt(s);
                    }
                });

        List<Tuple3<Long, long[], float[]>> resultsDesc = topkDesc.collect();

        Assert.assertEquals(resultsDesc.size(), 2);
        for (Tuple3<Long, long[], float[]> result : resultsDesc) {
            Assert.assertTrue(result.f0 == 0L || result.f0 == 1L);
            if (result.f0 == 0L) {
                Assert.assertArrayEquals(result.f1, new long[]{2L, 1L});
                Assert.assertArrayEquals(result.f2, new float[]{2.236068F, 2.0F}, 1.0e-6F);
            } else {
                Assert.assertArrayEquals(result.f1, new long[]{3L, 0L});
                Assert.assertArrayEquals(result.f2, new float[]{2.236068F, 2.0F}, 1.0e-6F);
            }
        }

        DataSet<Tuple3<Long, long[], float[]>> topkAsc = BlockwiseCross
            .findTopK(dataset1, dataset2, 2, Order.ASCENDING,
                new BlockwiseCross.ScoreFunction<float[], float[]>() {
                    @Override
                    public float score(Long id1, float[] v1, Long id2, float[] v2) {
                        float s = 0.F;
                        for (int i = 0; i < v1.length; i++) {
                            s += (v1[i] - v2[i]) * (v1[i] - v2[i]);
                        }
                        return (float) Math.sqrt(s);
                    }
                });

        List<Tuple3<Long, long[], float[]>> resultsAsc = topkAsc.collect();

        Assert.assertEquals(resultsAsc.size(), 2);
        for (Tuple3<Long, long[], float[]> result : resultsAsc) {
            Assert.assertTrue(result.f0 == 0L || result.f0 == 1L);
            if (result.f0 == 0L) {
                Assert.assertArrayEquals(result.f1, new long[]{0L, 3L});
                Assert.assertArrayEquals(result.f2, new float[]{1.F, 1.4142135F}, 1.0e-6F);
            } else {
                Assert.assertArrayEquals(result.f1, new long[]{1L, 2L});
                Assert.assertArrayEquals(result.f2, new float[]{1.F, 1.4142135F}, 1.0e-6F);
            }
        }
    }
}