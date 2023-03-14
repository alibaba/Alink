package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple5;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TreeParzenEstimatorTest extends AlinkTestBase {
	@Test
	public void test1() {
		// The target function is 2x^3+x^2-4x, and its minimum point is x=2/3
		double bestX = 2.0 / 3.0;
		double bestValue = func(bestX);

		int initNum = 10;
		int sampleNum = 10;
		int tryTimes = 0;
		// the search range is [0,20)
		double upperBound = 20;

		// list to store history records
		List <DenseVector> X = new ArrayList <>(initNum + 20);
		List <Double> y = new ArrayList <>(initNum + 20);
		Random rand = new Random();

		// Init data.
		for (int i = 0; i < initNum; i++) {
			double x = rand.nextDouble() * upperBound;
			while (func(x) < 5) {
				x = rand.nextDouble() * upperBound;
			}

			X.add(new DenseVector(new double[] {x}));
			y.add(func(x));
		}

		// train the tpe optimizer.
		ValueDist valueDist = ValueDist.uniform(0, upperBound);
		double minValue = Double.MAX_VALUE;
		double minX = 0;
		while (Math.abs(bestValue - minValue) > 0.1) {
			tryTimes++;
			TreeParzenEstimator tpe = new TreeParzenEstimator(X, y);

			double[] samples = new double[sampleNum];
			Tuple5 <Double, Double, Double, Double, Integer> summary = ValueDistUtils.getValueDistSummary(valueDist);
			for (int i = 0; i < sampleNum; i++) {
				samples[i] = (double) valueDist.get(rand.nextDouble());
				double x = samples[i];
			}

			double x = samples[tpe.calc(new DenseVector(samples), 0, summary.f0, summary.f1, summary.f2,
				summary.f3, summary.f4, 25)];
			double value = func(x);
			if (value < minValue) {
				minValue = value;
				minX = x;
			}

			X.add(new DenseVector(new double[] {x}));
			y.add(func(x));
		}

		System.out.printf(
			"The expected value is %f when param is %f.\nTPE found best value %f and best param %f.\nTried %d times\n",
			bestValue, bestX, minValue, minX, tryTimes);

		Assert.assertEquals(bestValue, minValue, 0.1);
	}
	
	@Test
	public void test2() {
		// The target function is y=sin(x)
		double bestX = Math.PI*1.5;
		double bestValue = Math.sin(bestX);

		int initNum = 10;
		int sampleNum = 10;
		int tryTimes = 0;
		// the search range is [0,20)
		double upperBound = Math.PI*2;

		// list to store history records
		List <DenseVector> X = new ArrayList <>(initNum + 20);
		List <Double> y = new ArrayList <>(initNum + 20);
		Random rand = new Random();

		// Init data.
		for (int i = 0; i < initNum; i++) {
			double x = rand.nextDouble() * upperBound;
			X.add(new DenseVector(new double[] {x}));
			y.add(Math.sin(x));
		}

		// train the tpe optimizer.
		ValueDist valueDist = ValueDist.uniform(0, upperBound);
		double minValue = Double.MAX_VALUE;
		double minX = 0;
		while (Math.abs(bestValue - minValue) > 0.1) {
			tryTimes++;
			TreeParzenEstimator tpe = new TreeParzenEstimator(X, y);

			double[] samples = new double[sampleNum];
			Tuple5 <Double, Double, Double, Double, Integer> summary = ValueDistUtils.getValueDistSummary(valueDist);
			for (int i = 0; i < sampleNum; i++) {
				samples[i] = (double) valueDist.get(rand.nextDouble());
				double x = samples[i];
			}

			double x = samples[tpe.calc(new DenseVector(samples), 0, summary.f0, summary.f1, summary.f2,
				summary.f3, summary.f4, 25)];
			double value = Math.sin(x);
			if (value < minValue) {
				minValue = value;
				minX = x;
			}

			X.add(new DenseVector(new double[] {x}));
			y.add(Math.sin(x));
		}

		System.out.printf(
			"The expected value is %f when param is %f.\nTPE found best value %f and best param %f.\nTried %d times\n",
			bestValue, bestX, minValue, minX, tryTimes);


	}

	private static double func(double x) {
		return 2 * Math.pow(x, 3) + Math.pow(x, 2) - 4 * x;
	}
}
