package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.google.common.base.Joiner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StratifiedSampleTest {

    @Test
    public void testStratifiedSampleWithRatio() throws Exception {
        BatchOperator.setParallelism(1);
        Random r = new Random();
        int size = 10000;
        String value1 = "a";
        List<Row> data = new ArrayList<>();
        for (int i = 0; i <size ; i++) {
            data.add(Row.of(value1, r.nextInt(100)));
        }
        double fraction = 0.5;
        MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[]{"key", "val"});
        StratifiedSampleBatchOp sampleOp = new StratifiedSampleBatchOp()
            .setGroupCol("key")
            .setRatios("a:0.5")
            .setWithReplacement(true);
        long count = sourceOp.link(sampleOp).count();
        Assert.assertTrue("expected number is :" + size * fraction + " +- " + size * 0.1 + " but actual is :" + count,
            count >= size * fraction * 0.9 && count <= size * fraction * 1.1);
    }

    @Test
    public void testStratifiedSampleWithRatio1() throws Exception{
        BatchOperator.setParallelism(3);
        Random r = new Random();
        int[] len = new int[]{2000,3000,4000};
        String[] keys = new String[]{"a","b","c"};
        List<Row> data = new ArrayList<>();
        for (int i = 0; i <len.length ; i++) {
            for (int j = 0; j <len[i] ; j++) {
                data.add(Row.of(keys[i], r.nextInt(100)));
            }
        }
        List<Tuple2<String, Double>> mappingFraction = Arrays.asList(new Tuple2<>("a",0.1),new Tuple2<>("b",0.2),new Tuple2<>("c",0.3));
        MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[]{"key", "val"});
        StratifiedSampleBatchOp sampleOp = new StratifiedSampleBatchOp()
            .setGroupCol("key")
            .setRatios(Joiner.on(",").join(mappingFraction.
                stream().
                map(t2 -> Joiner.on(":").join(t2.f0, t2.f1))
                .collect(Collectors.toList())))
            .setWithReplacement(true);
        Map<Object, Long> collect = sourceOp.link(sampleOp).
            collect().
            stream().
            map(e -> e.getField(0)).
            collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Long a = collect.get(keys[0]);
        Long b = collect.get(keys[1]);
        Long c = collect.get(keys[2]);
        Double fa = mappingFraction.get(0).getField(1);
        Double fb = mappingFraction.get(1).getField(1);
        Double fc = mappingFraction.get(2).getField(1);
        double f1 = len[0] * fa;
        double f2 = len[1] * fb;
        double f3 = len[2] * fc;
        Assert.assertTrue("expected number is :" + f1 + " +- " + f1 * 0.2 + " but actual is :" + a,
            a > f1 * 0.8 && a < f1 * 1.2);
        Assert.assertTrue("expected number is :" + f2 + " +- " + f2 * 0.2 + " but actual is :" + b,
            b > f2 * 0.8 && b < f2 * 1.2);
        Assert.assertTrue("expected number is :" + f3 + " +- " + f3 * 0.2 + " but actual is :" + c,
            c > f3 * 0.8 && c < f3 * 1.2);
    }


    @Test
    public void testStratifiedSampleWithSize() throws Exception {
        BatchOperator.setParallelism(1);
        Random r = new Random();
        int size = 500;
        String value1 = "a";
        List<Row> data = new ArrayList<>();
        for (int i = 0; i <size ; i++) {
            data.add(Row.of(value1, r.nextInt(100)));
        }
        long fraction = 100;
        MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[]{"key", "val"});
        StratifiedSampleWithSizeBatchOp sampleOp = new StratifiedSampleWithSizeBatchOp()
            .setGroupCol("key")
            .setSizes("a:100")
            .setWithReplacement(true);

        long count = sourceOp.link(sampleOp).count();
        Assert.assertEquals(fraction, count);
    }

    @Test
    public void testStratifiedSampleWithSize1() throws Exception{
        BatchOperator.setParallelism(3);
        Random r = new Random();
        int[] len = new int[]{2000,3000,4000};
        String[] keys = new String[]{"a","b","c"};
        List<Row> data = new ArrayList<>();
        for (int i = 0; i <len.length ; i++) {
            for (int j = 0; j <len[i] ; j++) {
                data.add(Row.of(keys[i], r.nextInt(100)));
            }
        }
        List<Tuple2<String, Long>> mappingFraction = Arrays.asList(new Tuple2<>("a",100L),new Tuple2<>("b",200L),new Tuple2<>("c",300L));
        MemSourceBatchOp sourceOp = new MemSourceBatchOp(data, new String[]{"key", "val"});
        StratifiedSampleWithSizeBatchOp sampleOp = new StratifiedSampleWithSizeBatchOp()
            .setGroupCol("key")
            .setSizes(Joiner.on(",").join(mappingFraction.
                stream().
                map(t2 -> Joiner.on(":").join(t2.f0, t2.f1))
                .collect(Collectors.toList())))
            .setWithReplacement(true);

        Map<Object, Long> collect = sourceOp.link(sampleOp).
            collect().
            stream().
            map(e -> e.getField(0)).
            collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Long a = collect.get(keys[0]);
        Long b = collect.get(keys[1]);
        Long c = collect.get(keys[2]);
        Long fa = mappingFraction.get(0).getField(1);
        Long fb = mappingFraction.get(1).getField(1);
        Long fc = mappingFraction.get(2).getField(1);

        Assert.assertEquals(a, fa);
        Assert.assertEquals(b, fb);
        Assert.assertEquals(c, fc);
    }
}
