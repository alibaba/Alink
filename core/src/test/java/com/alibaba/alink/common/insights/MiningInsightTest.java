package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

public class MiningInsightTest extends TestCase {

	public static Row[] CAR_SALES = new Row[] {
		Row.of("2007/1/1","BMW","Subcompact","BMW 1-Series",11018),
		Row.of("2008/1/1","BMW","Subcompact","BMW 1-Series",12018),
		Row.of("2009/1/1","BMW","Subcompact","BMW 1-Series",11182),
		Row.of("2010/1/1","BMW","Subcompact","BMW 1-Series",13132),
		Row.of("2011/1/1","BMW","Subcompact","BMW 1-Series",8832),
		Row.of("2007/1/1","BMW","SUV","BMW X3",28058),
		Row.of("2008/1/1","BMW","SUV","BMW X3",17622),
		Row.of("2009/1/1","BMW","SUV","BMW X3",6067),
		Row.of("2010/1/1","BMW","SUV","BMW X3",6075),
		Row.of("2011/1/1","BMW","SUV","BMW X3",27793),
		Row.of("2007/1/1","BMW","SUV","BMW X5",35202),
		Row.of("2008/1/1","BMW","SUV","BMW X5",31858),
		Row.of("2009/1/1","BMW","SUV","BMW X5",27071),
		Row.of("2010/1/1","BMW","SUV","BMW X5",35776),
		Row.of("2011/1/1","BMW","SUV","BMW X5",40547),
		Row.of("2007/1/1","BMW","SUV","BMW X6",2548),
		Row.of("2008/1/1","BMW","SUV","BMW X6",4548),
		Row.of("2009/1/1","BMW","SUV","BMW X6",4787),
		Row.of("2010/1/1","BMW","SUV","BMW X6",6257),
		Row.of("2011/1/1","BMW","SUV","BMW X6",6192),
		Row.of("2007/1/1","Ford","Compact","Focus",173213),
		Row.of("2008/1/1","Ford","Compact","Focus",195823),
		Row.of("2009/1/1","Ford","Compact","Focus",160433),
		Row.of("2010/1/1","Ford","Compact","Focus",172421),
		Row.of("2011/1/1","Ford","Compact","Focus",175717),
		Row.of("2007/1/1","Ford","Fullsize","Crown Victoria",60901),
		Row.of("2008/1/1","Ford","Fullsize","Crown Victoria",48557),
		Row.of("2009/1/1","Ford","Fullsize","Crown Victoria",33255),
		Row.of("2010/1/1","Ford","Fullsize","Crown Victoria",33722),
		Row.of("2011/1/1","Ford","Fullsize","Crown Victoria",46725)
	};
	public static String SCHEMA = "year_ string, brand string, category string, model string, sales int";

	@Test
	public void testCorrelation() {
		LocalOperator <?> source = new MemSourceLocalOp(CAR_SALES, SCHEMA);

		Subject subject = new Subject()
			.addSubspace(new Subspace("brand", "BMW"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("sales", MeasureAggr.SUM));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Correlation;
		insight.addAttachSubspace(new Subspace("brand", "BMW"))
			.addAttachSubspace(new Subspace("category", "SUV"));
		CorrelationInsight miningInsight = (CorrelationInsight) MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true).setNeedFilter(true);
		miningInsight.processData(source, source);
		System.out.println(miningInsight);
		System.out.println(miningInsight.insight.layout.data);
	}

	@Test
	public void testCrossMeaCor() {
		LocalOperator <?> source = new MemSourceLocalOp(CAR_SALES, SCHEMA);

		Subject subject = new Subject()
			.addSubspace(new Subspace("brand", "BMW"))
			.addSubspace(new Subspace("category", "SUV"))
			.setBreakdown(new Breakdown("model"))
			.addMeasure(new Measure("sales", MeasureAggr.AVG))
			.addMeasure(new Measure("sales", MeasureAggr.MIN));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.CrossMeasureCorrelation;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true).setNeedFilter(true);
		miningInsight.processData(source);
		System.out.println(miningInsight.insight);
	}

	public static Row[] EMISSION = new Row[] {
		Row.of("1990","AK","Commercial Cogen","Coal",821.929,13.191,3.009),
		Row.of("1991","AK","Commercial Cogen","Coal",848.745,8.359,3.146),
		Row.of("1992","AK","Commercial Cogen","Coal",860.878,8.469,3.195),
		Row.of("1993","AK","Commercial Cogen","Coal",858.244,8.393,3.208),
		Row.of("1994","AK","Commercial Cogen","Coal",866.661,8.88,3.238),
		Row.of("1995","AK","Commercial Cogen","Coal",883.639,7.824,1.893),
		Row.of("1996","AK","Commercial Cogen","Coal",855.508,7.927,1.871),
		Row.of("1997","AK","Commercial Cogen","Coal",900.737,8.132,1.899),
		Row.of("1998","AK","Commercial Cogen","Coal",640.626,4.802,1.327),
		Row.of("1999","AK","Commercial Cogen","Coal",658.91,4.702,1.366),
		Row.of("2000","AK","Commercial Cogen","Coal",647.281,3.813,0.871),
		Row.of("2001","AK","Commercial Cogen","Coal",595.987,0.714,0.863),
		Row.of("2002","AK","Commercial Cogen","Coal",390.357,0.621,0.767),
		Row.of("2003","AK","Commercial Cogen","Coal",703.098,1.29,1.405),
		Row.of("2004","AK","Commercial Cogen","Coal",705.588,0.901,0.815),
		Row.of("2005","AK","Commercial Cogen","Coal",699.467,1.401,1.537),
		Row.of("2006","AK","Commercial Cogen","Coal",692.025,1.422,1.561),
		Row.of("2007","AK","Commercial Cogen","Coal",704.9,1.455,1.598),
		Row.of("2008","AK","Commercial Cogen","Coal",750.852,1.791,1.682),
		Row.of("2009","AK","Commercial Cogen","Coal",720.938,1.713,1.604),
		Row.of("2010","AK","Commercial Cogen","Coal",723.229,1.615,1.615),
		Row.of("2011","AK","Commercial Cogen","Coal",827.001,1.744,1.844),
		Row.of("2012","AK","Commercial Cogen","Coal",830.471,1.788,1.73),
		Row.of("2013","AK","Commercial Cogen","Coal",349.232,0.775,0.796),
		Row.of("2014","AK","Commercial Cogen","Coal",341.306,0.702,0.775),
		Row.of("2015","AK","Commercial Cogen","Coal",345.371,0.701,0.633),
		Row.of("1990","AK","Commercial Cogen","Petroleum",2.075,0.006,0.002),
		Row.of("1991","AK","Commercial Cogen","Petroleum",2.677,0.008,0.003),
		Row.of("1992","AK","Commercial Cogen","Petroleum",2.542,0.008,0.002),
		Row.of("1993","AK","Commercial Cogen","Petroleum",1.509,0.005,0.001),
		Row.of("1994","AK","Commercial Cogen","Petroleum",7.849,0.025,0.007),
		Row.of("1995","AK","Commercial Cogen","Petroleum",3.865,0.008,0.006),
		Row.of("1996","AK","Commercial Cogen","Petroleum",3.735,0.008,0.007),
		Row.of("1997","AK","Commercial Cogen","Petroleum",33.821,0.07,0.587),
		Row.of("1998","AK","Commercial Cogen","Petroleum",0.805,0.002,0.001),
		Row.of("1999","AK","Commercial Cogen","Petroleum",7.43,0.018,0.172),
		Row.of("2000","AK","Commercial Cogen","Petroleum",10.798,0.025,0.249),
		Row.of("2001","AK","Commercial Cogen","Petroleum",4.017,0.018,0.097),
		Row.of("2002","AK","Commercial Cogen","Petroleum",14.233,0.036,0.22),
		Row.of("2003","AK","Commercial Cogen","Petroleum",11.546,0.028,0.087),
		Row.of("2004","AK","Commercial Cogen","Petroleum",17.57,0.049,0.103),
		Row.of("2005","AK","Commercial Cogen","Petroleum",14.246,0.034,0.103),
		Row.of("2006","AK","Commercial Cogen","Petroleum",10.279,0.03,0.042),
		Row.of("2007","AK","Commercial Cogen","Petroleum",13.481,0.032,0.058),
		Row.of("2008","AK","Commercial Cogen","Petroleum",9.177,0.034,0.028),
		Row.of("2009","AK","Commercial Cogen","Petroleum",13.261,0.034,0.055),
		Row.of("2010","AK","Commercial Cogen","Petroleum",9.98,0.007,0.031),
		Row.of("2011","AK","Commercial Cogen","Petroleum",4.87,0.003,0.01),
		Row.of("2012","AK","Commercial Cogen","Petroleum",10.431,0.0,0.046),
		Row.of("2013","AK","Commercial Cogen","Petroleum",7.749,0.014,0.016),
		Row.of("2014","AK","Commercial Cogen","Petroleum",5.24,0.009,0.007),
		Row.of("2015","AK","Commercial Cogen","Petroleum",10.25,0.018,0.015),
	};
	public static String EMISSION_SCHEMA = "year_ string, state string, producer_type string, energy_source string, co double, so double, nox double";

	@Test
	public void testCrossMeaCor2() {
		LocalOperator <?> source = new MemSourceLocalOp(EMISSION, EMISSION_SCHEMA);

		Subject subject = new Subject()
			.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.MAX))
			.addMeasure(new Measure("so", MeasureAggr.MAX));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.CrossMeasureCorrelation;

		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true).setNeedFilter(true);
		miningInsight.processData(source);
		System.out.println(insight);
		System.out.println(insight.layout.data);
	}

	@Test
	public void testCrossMeaCor3() {
		LocalOperator<?> data = Data.getEmissionLocalSource();

		Subject subject = new Subject()
			.setBreakdown(new Breakdown("EnergySource"))
			.addMeasure(new Measure("CO2_kt", MeasureAggr.AVG))
			.addMeasure(new Measure("SO2_kt", MeasureAggr.SUM));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.CrossMeasureCorrelation;

		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true).setNeedFilter(true);
		miningInsight.processData(data);
		System.out.println(insight);
	}

	@Test
	public void testCluster2DWithFilter() {
		LocalOperator <?> source = new MemSourceLocalOp(EMISSION, EMISSION_SCHEMA);

		Subject subject = new Subject()
			.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.AVG))
			.addMeasure(new Measure("so", MeasureAggr.MAX));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Clustering2D;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true);
		miningInsight.setNeedFilter(true);
		miningInsight.processData(source);
		System.out.println(miningInsight);
	}

	@Test
	public void testCluster2D() {
		LocalOperator <?> source = new MemSourceLocalOp(EMISSION, EMISSION_SCHEMA);

		Subject subject = new Subject()
			//.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.AVG))
			.addMeasure(new Measure("so", MeasureAggr.MAX));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Clustering2D;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true);
		//miningInsight.setNeedFilter(true);
		miningInsight.processData(source);
		System.out.println(miningInsight);
	}

	@Test
	public void testCluster() {
		ArrayList<Row> rows = new ArrayList <>();
		Random generator = new Random(11);
		for (int year = 1000; year < 1100; year++) {
			rows.add(Row.of(Math.random(), Math.random(), String.valueOf(year)));
		}
		for (int year = 2000; year < 2100; year++) {
			rows.add(Row.of(Math.random() + 10, Math.random() + 10, String.valueOf(year)));
		}
		LocalOperator <?> source = new MemSourceLocalOp(rows, "co double,so double,year_ string");

		Subject subject = new Subject()
			//.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.AVG))
			.addMeasure(new Measure("so", MeasureAggr.AVG));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Clustering2D;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true);
		miningInsight.processData(source);
		System.out.println(miningInsight);
	}

	@Test
	public void testCluster2() {
		ArrayList<Row> rows = new ArrayList <>();
		Random generator = new Random(11);
		for (int year = 2000; year < 2020; year++) {
			rows.add(Row.of(generator.nextDouble(), generator.nextDouble(), String.valueOf(year)));
		}
		LocalOperator <?> source = new MemSourceLocalOp(rows, "co double,so double,year_ string");

		Subject subject = new Subject()
			//.addSubspace(new Subspace("energy_source", "Petroleum"))
			.setBreakdown(new Breakdown("year_"))
			.addMeasure(new Measure("co", MeasureAggr.AVG))
			.addMeasure(new Measure("so", MeasureAggr.MAX));
		Insight insight = new Insight();
		insight.subject = subject;
		insight.type = InsightType.Clustering2D;
		CorrelationInsightBase miningInsight = MiningInsightFactory.getMiningInsight(insight);
		miningInsight.setNeedGroup(true);
		miningInsight.processData(source);
		System.out.println(miningInsight);
	}

}