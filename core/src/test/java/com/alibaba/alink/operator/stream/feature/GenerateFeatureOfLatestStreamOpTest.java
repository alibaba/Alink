package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.fe.define.InterfaceLatestStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestNCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestNCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestNNumericStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeIntervalCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeIntervalCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeIntervalNumericStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeSlotCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeSlotCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeSlotNumericStatFeatures;
import com.alibaba.alink.common.fe.define.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics.LastN;
import com.alibaba.alink.common.fe.define.statistics.NumericStatistics;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class GenerateFeatureOfLatestStreamOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1000), "a", 1, 10.0, 1.0, 21.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(2000), "a", 1, 11.0, 2.0, 22.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(3000), "a", 1, 12.0, 3.0, 23.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(4000), "a", 1, 13.0, 4.0, 24.0, "a1", "b2", "c1"),
			Row.of(1, new Timestamp(5000), "a", 1, 14.0, 5.0, 25.0, "a1", "b2", "c1"),
			Row.of(1, new Timestamp(6000), "a", 1, 15.0, 6.0, 26.0, "a2", "b2", "c1"),
			Row.of(1, new Timestamp(7000), "a", 1, 16.0, 7.0, 27.0, "a2", "b2", "c1"),
			Row.of(1, new Timestamp(8000), "a", 1, 17.0, 8.0, 28.0, "a2", "b2", "c1"),
			Row.of(1, new Timestamp(9000), "a", 1, 18.0, 9.0, 29.0, "a2", "b1", "c1"),
			Row.of(1, new Timestamp(10000), "a", 1, 19.0, 10.0, 30.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(11000), "a", 1, 10.0, 1.0, 21.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(12000), "a", 1, 11.0, 2.0, 22.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(13000), "a", 1, 12.0, 3.0, 23.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(14000), "a", 1, 13.0, 4.0, 24.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(15000), "a", 1, 14.0, 5.0, 25.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(16000), "a", 1, 15.0, 6.0, 26.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(17000), "a", 1, 16.0, 7.0, 27.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(18000), "a", 1, 17.0, 8.0, 28.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(19000), "a", 1, 18.0, 9.0, 29.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(20000), "a", 1, 19.0, 10.0, 30.0, "a1", "b1", "c1")
		);

		StreamOperator <?> source = new MemSourceStreamOp(mTableData, new String[] {"id", "ts",
			"g1", "g2", "n1", "n2", "n3", "c1", "c2", "c3"});

		//category
		String[] lcGroupCols = new String[] {"g1", "g2"};

		BaseCategoricalStatistics[] lcTypes = new BaseCategoricalStatistics[2];
		lcTypes[0] = CategoricalStatistics.COUNT;
		//lcTypes[1] = CategoricalStatisticsNames.TOTAL_COUNT;
		lcTypes[1] = new LastN(1);

		String[] cFeatureCols = new String[] {"c1", "c2", "c3"};

		LatestNCategoricalStatFeatures lcFeatures =
			new LatestNCategoricalStatFeatures()
				.setNumbers(3)
				.setCategoricalStatistics(lcTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(lcGroupCols);

		LatestTimeIntervalCategoricalStatFeatures ltiFeatures =
			new LatestTimeIntervalCategoricalStatFeatures()
				.setTimeIntervals("1h")
				.setCategoricalStatistics(lcTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(lcGroupCols);

		LatestTimeSlotCategoricalStatFeatures ltsFeatures = new LatestTimeSlotCategoricalStatFeatures()
			.setTimeSlots("1h")
			.setCategoricalStatistics(lcTypes)
			.setFeatureCols(cFeatureCols)
			.setGroupCols(lcGroupCols);

		//number
		BaseNumericStatistics[] lnTypes = new BaseNumericStatistics[2];
		lnTypes[0] = NumericStatistics.COUNT;
		lnTypes[1] = NumericStatistics.LAST_N(1);

		String[] nFeatureCols = new String[] {"n1", "n2", "n3"};

		LatestNNumericStatFeatures lnFeatures = new LatestNNumericStatFeatures()
			.setNumbers(3)
			.setNumericStatistics(lnTypes)
			.setFeatureCols(cFeatureCols)
			.setGroupCols(lcGroupCols);

		LatestTimeIntervalNumericStatFeatures ltnFeatures = new LatestTimeIntervalNumericStatFeatures()
			.setTimeIntervals("3s")
			.setNumericStatistics(lnTypes)
			.setFeatureCols(cFeatureCols)
			.setGroupCols(lcGroupCols);

		LatestTimeSlotNumericStatFeatures ltsnFeatures = new LatestTimeSlotNumericStatFeatures()
			.setTimeSlots("10s")
			.setNumericStatistics(lnTypes)
			.setFeatureCols(cFeatureCols)
			.setGroupCols(lcGroupCols);

		//cross
		BaseCategoricalStatistics[] ccTypes = new BaseCategoricalStatistics[2];
		ccTypes[0] = CategoricalStatistics.COUNT;
		ccTypes[1] = CategoricalStatistics.LAST_N(1);

		String[][] ccFeatureCols = new String[][] {
			{"c1", "c2"},
			{"c1", "c3"}
		};

		LatestNCrossCategoricalStatFeatures nccFeatures = new LatestNCrossCategoricalStatFeatures()
			.setNumbers(3)
			.setCategoricalStatistics(ccTypes)
			.setCrossFeatureCols(ccFeatureCols)
			.setGroupCols(lcGroupCols);

		LatestTimeIntervalCrossCategoricalStatFeatures lccFeatures =
			new LatestTimeIntervalCrossCategoricalStatFeatures()
				.setTimeIntervals("1h")
				.setCategoricalStatistics(ccTypes)
				.setCrossFeatureCols(ccFeatureCols)
				.setGroupCols(lcGroupCols);

		LatestTimeSlotCrossCategoricalStatFeatures tsccFeatures = new LatestTimeSlotCrossCategoricalStatFeatures()
			.setTimeSlots("1h")
			.setCategoricalStatistics(ccTypes)
			.setCrossFeatureCols(ccFeatureCols)
			.setGroupCols(lcGroupCols);

		InterfaceLatestStatFeatures[] features = new InterfaceLatestStatFeatures[] {
			lcFeatures,
			ltiFeatures,
			ltsFeatures,
			lnFeatures,
			ltnFeatures,
			ltsnFeatures,
			nccFeatures,
			lccFeatures,
			tsccFeatures
		};

		StreamOperator <?> gf = new GenerateFeatureOfLatestStreamOp()
			.setTimeCol("ts")
			.setFeatureDefinitions(features);


		source.link(gf).print();

		gf.getOutputTable().printSchema();


		StreamOperator.execute();

	}

}