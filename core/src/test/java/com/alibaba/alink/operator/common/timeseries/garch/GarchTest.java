package com.alibaba.alink.operator.common.timeseries.garch;

import com.alibaba.alink.common.linalg.DenseVector;
import org.junit.Test;

public class GarchTest {

	@Test
	public void test() {
		double[] data = new double[] {40.6, 41.5, 41.8, 42.5, 43.3, 43.4, 43.8, 44.2, 45.3, 45.6};

		GarchModel garchModel = Garch.fit(data, 1,1, true);

		for(String warn: garchModel.warn) {
			System.out.println(warn);
		}

		double[] pred = garchModel.forecast(4);
		System.out.println(new DenseVector(pred));
	}


	@Test
	public void test2() {
		double[] data = new double[] {34002,32962,36312,38725,38946,42767,38813,37026,36228,38404,
			36739,37798,36198,34757,33089,34821,76381,72847,76770,72637,63416,59350,62816,51849,
			46223,42275,37765,32305,31773,30401,32356,33962,38361,42725,41915,40839,37607,40688,
			42341,42676,37784,33290,33178,38902,40305,42502,40571,37007,33891,31486,32133,52188,
			74300,72416,73886,76997,66838,51097,47738,95033,88243,92592,84959,71047,53164,58806,
			57415,115252,65742,54093,50521,48097,44476,52632,120242,111584,181650,144878,102792,
			94398,85064,68090,60413,53281,47004,44886,46542,43183,42861,47976,62843,75449,75843,
			76860,59401,45445,43007,36851,31323,30936};

		GarchModel garchModel = Garch.fit(data, 1,1, true);

		for(String warn: garchModel.warn) {
			System.out.println(warn);
		}

		double[] pred = garchModel.forecast(4);
		System.out.println(new DenseVector(pred));
	}


}