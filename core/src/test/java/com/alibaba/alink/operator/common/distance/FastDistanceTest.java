package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.*;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Unit test for FastDistance.
 */
public class FastDistanceTest {
    @Test
    public void testDenseVectorRowInput(){
        Vector vec = DenseVector.rand(10);
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(vec, 0, "a"), 0, 1, 2);

        assertVectorInput(vectorData, vec, Row.of(0, "a"));
    }

    @Test
    public void testDenseVectorTupleInput(){
        Vector vec = DenseVector.rand(10);
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(vec, Row.of(0, "a")));

        assertVectorInput(vectorData, vec, Row.of(0, "a"));
    }

    @Test
    public void testDenseIterableRowInput(){
        int len = 10000;
        int size = 100;
        List<Vector> vectorList = initDenseData(len, size);
        List<Row> rows = new ArrayList<>();
        for(int i = 0; i < vectorList.size(); i++){
            rows.add(Row.of(i, vectorList.get(i)));
        }

        EuclideanDistance distance = new EuclideanDistance();
        List<FastDistanceData> list = distance.prepareMatrixData(rows, 1, 0);

        assertIterableDenseInput(list, len, size, vectorList);
    }

    @Test
    public void testDenseIterableTupleInput(){
        int len = 10000;
        int size = 100;
        List<Vector> vectorList = initDenseData(len, size);
        List<Tuple2<Vector, Row>> rows = new ArrayList<>();
        for(int i = 0; i < vectorList.size(); i++){
            rows.add(Tuple2.of(vectorList.get(i), Row.of(i)));
        }

        EuclideanDistance distance = new EuclideanDistance();
        List<FastDistanceData> list = distance.prepareMatrixData(rows);

        assertIterableDenseInput(list, len, size, vectorList);
    }

    @Test
    public void testSparseVectorRowInput(){
        Vector vec = new SparseVector(10, new int[]{1, 2}, new double[]{1.0, 1.0});
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceVectorData vectorData = distance.prepareVectorData(Row.of(vec, 0, "a"), 0, 1, 2);

        assertVectorInput(vectorData, vec, Row.of(0, "a"));
    }

    @Test
    public void testSparseVectorTupleInput(){
        Vector vec = new SparseVector(10, new int[]{1, 2}, new double[]{1.0, 1.0});
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceVectorData vectorData = distance.prepareVectorData(Tuple2.of(vec, Row.of(0, "a")));

        assertVectorInput(vectorData, vec, Row.of(0, "a"));
    }

    @Test
    public void testSparseIterableRowInput(){
        int len = 20;
        int size = 50;
        List<Vector> vectorList = initSparseData(len, size);
        List<Row> rows = new ArrayList<>();
        for(int i = 0; i < vectorList.size(); i++){
            rows.add(Row.of(i, vectorList.get(i)));
        }
        EuclideanDistance distance = new EuclideanDistance();
        List<FastDistanceData> list = distance.prepareMatrixData(rows, 1, 0);

        assertIterableSparseInput(list, len, vectorList);
    }

    @Test
    public void testSparseIterableTupleInput(){
        int len = 20;
        int size = 50;
        List<Vector> vectorList = initSparseData(len, size);
        List<Tuple2<Vector, Row>> rows = new ArrayList<>();
        for(int i = 0; i < vectorList.size(); i++){
            rows.add(Tuple2.of(vectorList.get(i), Row.of(i)));
        }
        EuclideanDistance distance = new EuclideanDistance();
        List<FastDistanceData> list = distance.prepareMatrixData(rows);
        assertIterableSparseInput(list, len, vectorList);
    }

    @Test
    public void testCalVecVec(){
        Vector vec = DenseVector.rand(10);
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceData vectorData = distance.prepareVectorData(Tuple2.of(vec, null));
        DenseMatrix res = distance.calc(vectorData, vectorData);
        Assert.assertEquals(res.numRows(), 1);
        Assert.assertEquals(res.numCols(), 1);
    }

    @Test
    public void testCalVecMatrix(){
        int len = 20;
        int size = 10;
        Vector vec = DenseVector.rand(10);
        List<Vector> vectorList = initDenseData(len, size);
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceData vectorData = distance.prepareVectorData(Tuple2.of(vec, null));
        FastDistanceData matrixData = distance.prepareMatrixData(Iterables.transform(vectorList, v -> Tuple2.of(v, null))).get(0);
        DenseMatrix res = distance.calc(vectorData, matrixData);
        Assert.assertEquals(res.numRows(), len);
        Assert.assertEquals(res.numCols(), 1);
        DenseMatrix resCopy = res;

        Vector newVec = DenseVector.rand(10);
        FastDistanceData newVectorData = distance.prepareVectorData(Tuple2.of(newVec, null));
        DenseMatrix sameRes = distance.calc(newVectorData, matrixData, res);
        Assert.assertSame(sameRes, resCopy);

        DenseMatrix newRes = distance.calc(matrixData, newVectorData, res);
        Assert.assertNotSame(newRes, resCopy);
        Assert.assertEquals(newRes.numRows(), 1);
        Assert.assertEquals(newRes.numCols(), len);
    }

    @Test
    public void testCalMatrixMatrix(){
        int len = 20;
        int size = 10;
        List<Vector> vectorList1 = initDenseData(len, size);
        List<Vector> vectorList2 = initDenseData(len * 2, size);
        EuclideanDistance distance = new EuclideanDistance();
        FastDistanceData matrixData1 = distance.prepareMatrixData(Iterables.transform(vectorList1, v -> Tuple2.of(v, null))).get(0);
        FastDistanceData matrixData2 = distance.prepareMatrixData(Iterables.transform(vectorList2, v -> Tuple2.of(v, null))).get(0);

        DenseMatrix res = distance.calc(matrixData1, matrixData2);
        Assert.assertEquals(res.numRows(), len * 2);
        Assert.assertEquals(res.numCols(), len);
        DenseMatrix resCopy = res;

        DenseMatrix sameRes = distance.calc(matrixData1, matrixData2, res);
        Assert.assertSame(resCopy, sameRes);

        DenseMatrix newRes = distance.calc(matrixData2, matrixData1, res);
        Assert.assertNotSame(newRes, resCopy);
        Assert.assertEquals(newRes.numRows(), len);
        Assert.assertEquals(newRes.numCols(), len * 2);
    }

    private List<Vector> initSparseData(int len, int size){
        Random random = new Random();
        List<Vector> list = new ArrayList<>();
        for(int i = 0; i < len; i++){
            list.add(new SparseVector(size, new int[]{i, i + 1}, new double[]{random.nextDouble(), random.nextDouble()}));
        }
        return list;
    }

    private List<Vector> initDenseData(int len, int size){
        List<Vector> list = new ArrayList<>();
        for(int i = 0; i < len; i++){
            list.add(DenseVector.rand(size));
        }
        return list;
    }

    private void assertVectorInput(FastDistanceVectorData vectorData, Vector vec, Row row){
        Assert.assertEquals(vectorData.rows.length, 1);
        Assert.assertEquals(vectorData.rows[0], row);
        Assert.assertEquals(vectorData.vector, vec);
    }

    private void assertIterableSparseInput(List<FastDistanceData> list, int len, List<Vector> vectorList){
        Assert.assertEquals(list.size(), len);
        for(int i = 0; i < list.size(); i++){
            FastDistanceData data = list.get(i);
            Assert.assertTrue(data instanceof FastDistanceVectorData);
            Assert.assertEquals(data.rows.length, 1);
            Assert.assertEquals(data.rows[0], Row.of(i));
            FastDistanceVectorData vectorData = (FastDistanceVectorData)data;
            Assert.assertEquals(vectorData.vector, VectorUtil.getVector(vectorList.get(i)));
        }
    }

    private void assertIterableDenseInput(List<FastDistanceData> list, int len, int size, List<Vector> vectorList){
        Assert.assertEquals(list.size(), 2);
        Assert.assertTrue(list.get(0) instanceof FastDistanceMatrixData);
        Assert.assertTrue(list.get(1) instanceof FastDistanceMatrixData);
        FastDistanceMatrixData matrixData1 = (FastDistanceMatrixData)list.get(0);
        FastDistanceMatrixData matrixData2 = (FastDistanceMatrixData)list.get(1);

        Assert.assertEquals(matrixData1.rows.length + matrixData2.rows.length, len);
        Assert.assertEquals(matrixData1.vectors.numCols() + matrixData2.vectors.numCols(), len);
        Assert.assertEquals(matrixData1.vectors.numRows(), size);
        Assert.assertEquals(matrixData2.vectors.numRows(), size);

        int cnt = 0;
        for(int i = 0; i < matrixData1.rows.length; i++){
            Assert.assertEquals(matrixData1.rows[i].getArity(), 1);
            Assert.assertEquals(matrixData1.rows[i], Row.of(cnt));
            Assert.assertEquals(new DenseVector(matrixData1.vectors.getColumn(i)), vectorList.get(cnt++));
        }

        for(int i = 0; i < matrixData2.rows.length; i++){
            Assert.assertEquals(matrixData2.rows[i].getArity(), 1);
            Assert.assertEquals(matrixData2.rows[i], Row.of(cnt));
            Assert.assertEquals(new DenseVector(matrixData2.vectors.getColumn(i)), vectorList.get(cnt++));
        }
    }

}