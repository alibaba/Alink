package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GaussianProcessRegressionTest extends AlinkTestBase {
	@Test
	public void testGaussianProcessRegression(){
		double bestX = Math.PI/2;
		double bestValue = Math.sin(bestX);

		int initNum = 10;
		int sampleNum = 10;
		int tryTimes = 0;
		// the search range is [0,2*pi)
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
		double maxValue = Double.MIN_VALUE;
		double sample=0;
		double maxReg=Double.MIN_VALUE;
		
		while (Math.abs(bestValue - maxValue) > 0.1 && tryTimes<100) {
			tryTimes++;
			GaussianProcessRegression gp = new GaussianProcessRegression(X, y);
			for (int i = 0; i < sampleNum; i++) {
				double x = (double) valueDist.get(rand.nextDouble());
				double reg = gp.calc(new DenseVector(new double[]{x})).f0.doubleValue();
				if(reg>maxReg){
					maxReg = reg;
					sample = x;
					if(Math.sin(sample)>maxValue ){
						maxValue = Math.sin(sample);
					}
				}
			}
		}

		System.out.printf(
			"The expected value is %f when param is %f.\nTPE found best value %f and best param %f.\nTried %d times\n",
			bestValue, bestX, maxValue, sample, tryTimes);
	}
}
