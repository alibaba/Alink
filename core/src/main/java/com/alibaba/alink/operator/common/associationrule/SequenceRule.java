package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * The class for generating association rules given the frequent sequence patterns.
 */
public class SequenceRule {

    /**
     * Generate sequence rules from frequent sequence patterns.
     *
     * @return dataset of tuples of: antecedent(left hand side), consequent(right hand side),
     * support count, [support, confidence])
     */
    public static DataSet<Tuple4<int[], int[], Integer, double[]>>
    extractSequenceRules(DataSet<Tuple2<int[], Integer>> sequencePatterns, DataSet<Long> sequenceCount,
                         final double minConfidence) {
        DataSet<Tuple3<int[], int[], Integer>> candidates = sequencePatterns
            .flatMap(new FlatMapFunction<Tuple2<int[], Integer>, Tuple3<int[], int[], Integer>>() {
                @Override
                public void flatMap(Tuple2<int[], Integer> value, Collector<Tuple3<int[], int[], Integer>> out) throws Exception {
                    int[] sequence = value.f0;
                    if (getSequenceLength(sequence) <= 1) {
                        return;
                    }
                    Tuple2<int[], int[]> splited = splitSequence(sequence);
                    out.collect(Tuple3.of(splited.f0, splited.f1, value.f1));
                }
            })
            .name("gen_rules_candidates");

        DataSet<Tuple4<int[], int[], Integer, Integer>> candidatesSupport = candidates
            .join(sequencePatterns)
            .where(0).equalTo(0)
            .projectFirst(0, 1, 2).projectSecond(1);

        return candidatesSupport
            .flatMap(
                new RichFlatMapFunction<Tuple4<int[], int[], Integer, Integer>, Tuple4<int[], int[], Integer, double[]>>() {
                    transient Long transactionCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        List<Long> bcTranCnt = getRuntimeContext().getBroadcastVariable("sequenceCount");
                        this.transactionCount = bcTranCnt.get(0);
                    }

                    @Override
                    public void flatMap(Tuple4<int[], int[], Integer, Integer> value,
                                        Collector<Tuple4<int[], int[], Integer, double[]>> out) throws Exception {
                        Integer supportXY = value.f2;
                        Integer supportX = value.f3;
                        double confidence = supportXY.doubleValue() / supportX.doubleValue();
                        double support = supportXY.doubleValue() / transactionCount;
                        if (confidence >= minConfidence) {
                            out.collect(Tuple4.of(value.f0, value.f1, value.f2, new double[]{support, confidence}));
                        }
                    }
                })
            .withBroadcastSet(sequenceCount, "sequenceCount")
            .name("filter_rules");
    }

    private static int getSequenceLength(int[] sequence) {
        int n = 0;
        for (int aSequence : sequence) {
            if (aSequence == 0) {
                n++;
            }
        }
        return n - 1;
    }

    private static Tuple2<int[], int[]> splitSequence(int[] sequence) {
        int lastZeroPos = 0;
        for (int i = 0; i < sequence.length - 1; i++) {
            if (sequence[i] == 0) {
                lastZeroPos = i;
            }
        }
        int[] antecedent = Arrays.copyOfRange(sequence, 0, lastZeroPos + 1);
        int[] consequent = Arrays.copyOfRange(sequence, lastZeroPos, sequence.length);
        return Tuple2.of(antecedent, consequent);
    }
}