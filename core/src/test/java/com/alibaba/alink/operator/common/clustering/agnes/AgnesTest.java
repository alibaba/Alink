package com.alibaba.alink.operator.common.clustering.agnes;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for Agnes
 */
public class AgnesTest extends AlinkTestBase {
	private List <AgnesSample> list;
	private List <AgnesSample> sampleList;

	@Before
	public void init() {
		list = new ArrayList <>();
		for (int i = 0; i < 20; i++) {
			list.add(new AgnesSample("id" + i, i, new DenseVector(new double[] {i, i + 1, i * 2, i / 2}), 1.0));
		}
		sampleList = new ArrayList <>();
		sampleList.add(new AgnesSample("a", 1L, new DenseVector(new double[] {2, 3}), 1.0));
		sampleList.add(new AgnesSample("b", 2L, new DenseVector(new double[] {2, 4}), 1.0));
		sampleList.add(new AgnesSample("c", 3L, new DenseVector(new double[] {1, 3}), 1.0));
		sampleList.add(new AgnesSample("d", 4L, new DenseVector(new double[] {1, 4}), 1.0));
		sampleList.add(new AgnesSample("e", 5L, new DenseVector(new double[] {2, 2}), 1.0));
		sampleList.add(new AgnesSample("f", 6L, new DenseVector(new double[] {3, 2}), 1.0));

		sampleList.add(new AgnesSample("g", 7L, new DenseVector(new double[] {8, 7}), 1.0));
		sampleList.add(new AgnesSample("h", 8L, new DenseVector(new double[] {7, 7}), 1.0));
		sampleList.add(new AgnesSample("i", 9L, new DenseVector(new double[] {8, 6}), 1.0));
		sampleList.add(new AgnesSample("j", 10L, new DenseVector(new double[] {7, 6}), 1.0));
		sampleList.add(new AgnesSample("k", 11L, new DenseVector(new double[] {8, 5}), 1.0));
	}

	@Test
	public void testAgnes() {
		List <AgnesCluster> res = Agnes.startAnalysis(list, 3, 3.0, Linkage.MIN, new EuclideanDistance());
		Assert.assertEquals(res.size(), 3);
		Assert.assertEquals(res.get(0).getAgnesSamples().size(), 16);
		for (AgnesCluster cluster : res) {
			for (AgnesSample sample : cluster.getAgnesSamples()) {
				System.out.println(
					"sampleId:" + sample.getSampleId() + ",clusterId: " + sample.getClusterId() + ", mergeIter: "
						+ sample.getMergeIter() + ", parentId: " + sample.getParentId());
			}
		}
	}

	@Test
	public void testMin() {
		List <AgnesCluster> clusters = Agnes.startAnalysis(sampleList, 2, 11, Linkage.MIN, new EuclideanDistance());
		Assert.assertEquals(clusters.get(0).getAgnesSamples().size(), 6);
		Assert.assertEquals(clusters.get(1).getAgnesSamples().size(), 5);
	}

	@Test
	public void testMax() {
		List <AgnesCluster> clusters = Agnes.startAnalysis(sampleList, 2, 11, Linkage.MAX, new EuclideanDistance());
		Assert.assertEquals(clusters.get(0).getAgnesSamples().size(), 6);
		Assert.assertEquals(clusters.get(1).getAgnesSamples().size(), 5);
	}

	@Test
	public void testAverage() {
		List <AgnesCluster> clusters = Agnes.startAnalysis(sampleList, 2, 11, Linkage.AVERAGE, new EuclideanDistance
			());
		Assert.assertEquals(clusters.get(0).getAgnesSamples().size(), 6);
		Assert.assertEquals(clusters.get(1).getAgnesSamples().size(), 5);
	}

	@Test
	public void testMean() {
		List <AgnesCluster> clusters = Agnes.startAnalysis(sampleList, 2, 11, Linkage.MEAN, new EuclideanDistance());
		Assert.assertEquals(clusters.get(0).getAgnesSamples().size(), 6);
		Assert.assertEquals(clusters.get(1).getAgnesSamples().size(), 5);
	}

}