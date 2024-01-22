package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.utils.Stopwatch;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.CsvSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ImpactDetectorTest extends TestCase {

	@Test
	public void testCarSales() {
		LocalOperator <?> data = Data.getCarSalesLocalSource();

		Stopwatch sw = new Stopwatch();
		sw.start();

		ImpactDetector impactDetector = new ImpactDetector(0.03);
		impactDetector.detect(data, LocalOperator.getParallelism());
		System.out.println(impactDetector.predict(new Subspace("brand", "BMW")));

		List <Tuple2 <Subspace, Double>> list = impactDetector.listSingleSubspace();
		for (Tuple2 <Subspace, Double> t2 : list) {
			System.out.println(t2.f0.colName + "->" + t2.f0.value.toString() + " : " + t2.f1);
		}

		List <Tuple3 <Subspace, Subspace, Double>> list2 = impactDetector.searchDoubleSubspace(data);
		for (Tuple3 <Subspace, Subspace, Double> t2 : list2) {
			System.out.println("{" + t2.f0.colName + "->" + t2.f0.value.toString() + ", \t" + t2.f1.colName + "->"
				+ t2.f1.value.toString() + "} : " + t2.f2);
		}

		BreakdownDetector breakdownDetector = new BreakdownDetector().detect(data, new ArrayList <>(),
			false, 100, LocalOperator.getParallelism());
		for (Tuple2 <Breakdown, List <Measure>> t2 : breakdownDetector.list) {
			System.out.print(t2.f0.colName + " : \t");
			for (Measure measure : t2.f1) {
				System.out.print(measure.colName + "." + measure.aggr.name() + ", ");
			}
			System.out.println();
		}

		sw.stop();
		System.out.println(sw.getElapsedTimeSpan());
	}

}