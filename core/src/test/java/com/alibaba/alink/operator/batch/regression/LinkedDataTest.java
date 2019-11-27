package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.regression.isotonicReg.LinkedData;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test of LinkedData.
 */
public class LinkedDataTest {

    private List<Tuple3<Double, Double, Double>> listData = new ArrayList<>();

    private void generateData() {
        listData.add(Tuple3.of(1.0, 0.02, 1.0));
        listData.add(Tuple3.of(1.0, 0.1, 1.0));
        listData.add(Tuple3.of(0.0, 0.18, 1.0));
        listData.add(Tuple3.of(0.0, 0.2, 1.0));
    }

    @Test
    public void putDataTest() {
        generateData();
        LinkedData linkedData = new LinkedData(listData);
        byte[] bytes = linkedData.getByteArray();
        //test getData
        Tuple4<Float, Double, Double, Float> data = linkedData.getData();
        Assert.assertEquals(data, Tuple4.of(1.0f, 0.02, 0.02, 1.0f));
        //test putData
        linkedData.putData(1.0f, 2.0, 3.0, 4.0f);
        Tuple4<Float, Double, Double, Float> newData = linkedData.getData();
        Assert.assertEquals(newData, Tuple4.of(1.0f, 2.0, 3.0, 4.0f));
        //test advance
        linkedData.advance();
        Tuple4<Float, Double, Double, Float> advanceData = linkedData.getData();
        Assert.assertEquals(advanceData, Tuple4.of(1.0f, 0.1, 0.1, 1.0f));
        //test retreat
        linkedData.retreat();
        Tuple4<Float, Double, Double, Float> retreatData = linkedData.getData();
        Assert.assertEquals(retreatData, Tuple4.of(1.0f, 2.0, 3.0, 4.0f));
        //test removeCurrentAndRetreat
        linkedData.advance();
        linkedData.removeCurrentAndRetreat();
        Tuple4<Float, Double, Double, Float> firstData = linkedData.getData();
        Assert.assertEquals(firstData, Tuple4.of(1.0f, 2.0, 3.0, 4.0f));
        linkedData.advance();
        Tuple4<Float, Double, Double, Float> thirdData = linkedData.getData();
        Assert.assertEquals(thirdData, Tuple4.of(0.0f, 0.18, 0.18, 1.0f));
        //delete the last one
        linkedData.advance();
        linkedData.removeCurrentAndRetreat();
        Tuple4<Float, Double, Double, Float> lastTwoData = linkedData.getData();
        Assert.assertEquals(lastTwoData, Tuple4.of(0.0f, 0.18, 0.18, 1.0f));
        //delete the first one
        linkedData.removeCurrentAndRetreat();
        Tuple4<Float, Double, Double, Float> theOnlyData = linkedData.getData();
        Assert.assertEquals(theOnlyData, Tuple4.of(1.0f, 2.0, 3.0, 4.0f));
        //test compact
        Assert.assertEquals(linkedData.compact(), 1);
    }

    @Test
    public void emptyArrayTest() {
        LinkedData linkedData = new LinkedData(new ArrayList<>());
        Assert.assertNull(linkedData.getByteArray());

    }
}
