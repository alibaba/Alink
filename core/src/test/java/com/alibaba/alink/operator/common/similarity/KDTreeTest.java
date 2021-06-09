package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KDTreeTest extends AlinkTestBase {
	private static EuclideanDistance distance = new EuclideanDistance();

	@Test
	public void buildTest() {
		FastDistanceVectorData[] tuples = new FastDistanceVectorData[30];
		for (int i = 0; i < tuples.length; i++) {
			Vector vec = DenseVector.ones(2);
			vec.scaleEqual(i);
			tuples[i] = distance.prepareVectorData(Tuple2.of(vec, Row.of(i)));
		}
		KDTree kdTree = new KDTree(tuples, 2, distance);
		KDTree.TreeNode root = kdTree.recursiveBuild(0, tuples.length);
		System.out.println(root.downThre);
	}

	@Test
	public void findBoundsTest() {
		FastDistanceVectorData[] tuples = new FastDistanceVectorData[30];
		for (int i = 0; i < tuples.length; i++) {
			Vector vec = DenseVector.ones(2);
			vec.scaleEqual(i);
			tuples[i] = distance.prepareVectorData(Tuple2.of(vec, Row.of(i)));
		}
		KDTree kdTree = new KDTree(tuples, 2, new EuclideanDistance());
		KDTree.TreeNode node = new KDTree.TreeNode();
		node.startIndex = 0;
		node.endIndex = 10;
		node.splitDim = 1;
		kdTree.findBounds(node);
		System.out.println(node);
	}

	@Test
	public void pickSplitDim() {
		FastDistanceVectorData[] tuples = new FastDistanceVectorData[30];
		DenseVector vec = new DenseVector(new double[] {1, 2});
		for (int i = 0; i < tuples.length; i++) {
			Vector vec1 = new DenseVector(new double[] {i, i * 2});
			tuples[i] = distance.prepareVectorData(Tuple2.of(vec1, Row.of(i)));
		}
		KDTree kdTree = new KDTree(tuples, 2, new EuclideanDistance());
		Assert.assertEquals(kdTree.pickSplitDim(0, 30), 1);
	}

	@Test
	public void rangeSearchTest() {
		FastDistanceVectorData[] tuples = new FastDistanceVectorData[30];
		DenseVector vec = DenseVector.rand(10);
		EuclideanDistance distance = new EuclideanDistance();
		for (int i = 0; i < tuples.length; i++) {
			DenseVector v = DenseVector.rand(10);
			tuples[i] = distance.prepareVectorData(Tuple2.of(v, Row.of(i)));
		}
		KDTree kdTree = new KDTree(tuples, 10, new EuclideanDistance());
		kdTree.buildTree();
		List <FastDistanceVectorData> list = kdTree.rangeSearch(1.1,
			distance.prepareVectorData(Tuple2.of(vec, Row.of(0))));

		List <Integer> predict = new ArrayList <>();
		for (int i = 0; i < tuples.length; i++) {
			if (distance.calc(tuples[i].getVector(), vec) <= 1.1) {
				predict.add((int) tuples[i].getRows()[0].getField(0));
			}
		}
		Assert.assertEquals(list.size(), predict.size());
	}

	@Test
	public void split() {
		FastDistanceVectorData[] tuples = new FastDistanceVectorData[30];
		for (int i = 0; i < tuples.length; i++) {
			tuples[i] = distance.prepareVectorData(Tuple2.of(DenseVector.ones(2), Row.of(i)));
		}
		KDTree kdTree = new KDTree(tuples, 2, new EuclideanDistance());
		for (int i = 0; i < kdTree.samples.length; i++) {
			System.out.println(kdTree.samples[i]);
		}
	}

	@Test
	public void test1() {
		Row[] array3 = new Row[] {
			Row.of("id_1", "1,1"),
			Row.of("id_2", "1,2"),
			Row.of("id_3", "2,1"),
			Row.of("id_4", "2,2"),
			Row.of("id_5", "2,3"),
			Row.of("id_6", "3,3"),
			Row.of("id_7", "3,4"),
			Row.of("id_8", "4,3"),
			Row.of("id_9", "1,3"),
			Row.of("id_10", "1,4"),
			Row.of("id_11", "3,2"),
			Row.of("id_12", "4,2"),
			Row.of("id_13", "2,4"),
			Row.of("id_14", "3,1"),
			Row.of("id_15", "4,4"),
			Row.of("id_16", "4,5"),
			Row.of("id_17", "5,5"),
			Row.of("id_18", "3,5"),
			Row.of("id_19", "5,6"),
			Row.of("id_20", "4,6"),
			Row.of("id_21", "3,5"),
			Row.of("id_22", "5,3")
		};

		FastDistanceVectorData[] tuples = new FastDistanceVectorData[array3.length];
		EuclideanDistance distance = new EuclideanDistance();
		for (int i = 0; i < tuples.length; i++) {
			DenseVector v = VectorUtil.parseDense((String) array3[i].getField(1));
			tuples[i] = distance.prepareVectorData(Tuple2.of(v, Row.of(i, array3[i].getField(0))));
		}
		DenseVector vec = new DenseVector(new double[] {4, 3});
		KDTree kdTree = new KDTree(tuples, 2, new EuclideanDistance());
		kdTree.buildTree();
		//List<Integer> list = kdTree.rangeSearch(1.2, new FastDistanceVectorData(Tuple2.of(vec, distance.vectorLabel
		// (vec)), 0, null));
		//System.out.println(list);

		System.out.println(Arrays.toString(kdTree.getTopN(8, distance.prepareVectorData(Tuple2.of(vec, null)))));
	}

}