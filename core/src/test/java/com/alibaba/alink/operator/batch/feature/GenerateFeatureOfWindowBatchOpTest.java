package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.fe.def.InterfaceWindowStatFeatures;
import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.def.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.def.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.def.statistics.CategoricalStatistics.LastN;
import com.alibaba.alink.common.fe.def.statistics.NumericStatistics;
import com.alibaba.alink.common.fe.def.window.HopWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.HopWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.HopWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.SessionWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.SessionWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.SessionWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.TumbleWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.TumbleWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.TumbleWindowNumericStatFeatures;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.GenerateFeatureOfWindowBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class GenerateFeatureOfWindowBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, new Timestamp(1000), "a", 1, 10.0, 1.0, 21.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(2000), "a", 1, 11.0, 2.0, 22.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(3000), "a", 1, 12.0, 3.0, 23.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(4000), "a", 1, 13.0, 4.0, 24.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(5000), "a", 1, 14.0, 5.0, 25.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(6000), "a", 1, 15.0, 6.0, 26.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(7000), "a", 1, 16.0, 7.0, 27.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(8000), "a", 1, 17.0, 8.0, 28.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(9000), "a", 1, 18.0, 9.0, 29.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(10000), "a", 1, 19.0, 10.0, 30.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(11000), "a", 1, 10.0, 1.0, 21.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(12000), "a", 1, 11.0, 2.0, 22.0, "a1", "b1", "c1"),
			//Row.of(1, new Timestamp(13000), "a", 1, 12.0, 3.0, 23.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(14000), "a", 1, 13.0, 4.0, 24.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(15000), "a", 1, 14.0, 5.0, 25.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(16000), "a", 1, 15.0, 6.0, 26.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(17000), "a", 1, 16.0, 7.0, 27.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(18000), "a", 1, 17.0, 8.0, 28.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(19000), "a", 1, 18.0, 9.0, 29.0, "a1", "b1", "c1"),
			Row.of(1, new Timestamp(20000), "a", 1, 19.0, 10.0, 30.0, "a1", "b1", "c1")
		);

		String[] l2GroupCols = new String[] {"g1", "g2"};
		String[] l1GroupCols = new String[] {"g2"};
		//category
		BatchOperator <?> source = new MemSourceBatchOp(mTableData, new String[] {"id", "ts",
			"g1", "g2", "n1", "n2", "n3", "c1", "c2", "c3"});

		BaseCategoricalStatistics[] lcTypes = new BaseCategoricalStatistics[2];
		lcTypes[0] = CategoricalStatistics.COUNT;
		lcTypes[1] = new LastN(1);

		String[] cFeatureCols = new String[] {"c1", "c2", "c3"};

		//tumble window
		TumbleWindowCategoricalStatFeatures twcFeature =
			new TumbleWindowCategoricalStatFeatures()
				.setWindowTimes("5s")
				.setCategoricalStatistics(lcTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		HopWindowCategoricalStatFeatures hpcFeature =
			new HopWindowCategoricalStatFeatures()
				.setWindowTimes("5s")
				.setHopTimes("2s")
				.setCategoricalStatistics(lcTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		SessionWindowCategoricalStatFeatures swcFeature =
			new SessionWindowCategoricalStatFeatures()
				.setSessionGapTimes("5s")
				.setCategoricalStatistics(lcTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		//number
		BaseNumericStatistics[] lnTypes = new BaseNumericStatistics[2];
		lnTypes[0] = NumericStatistics.COUNT;
		lnTypes[1] = NumericStatistics.LAST_N(1);

		String[] nFeatureCols = new String[] {"n1", "n2", "n3"};

		TumbleWindowNumericStatFeatures twnFeature =
			new TumbleWindowNumericStatFeatures()
				.setWindowTimes("5s")
				.setNumericStatistics(lnTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		HopWindowNumericStatFeatures hwnFeature =
			new HopWindowNumericStatFeatures()
				.setWindowTimes("5s")
				.setHopTimes("2s")
				.setNumericStatistics(lnTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		SessionWindowNumericStatFeatures swnFeature =
			new SessionWindowNumericStatFeatures()
				.setSessionGapTimes("1s")
				.setNumericStatistics(lnTypes)
				.setFeatureCols(cFeatureCols)
				.setGroupCols(l2GroupCols);

		//cross category
		BaseCategoricalStatistics[] ccTypes = new BaseCategoricalStatistics[2];
		ccTypes[0] = CategoricalStatistics.COUNT;
		ccTypes[1] = CategoricalStatistics.LAST_N(1);

		String[][] ccFeatureCols = new String[][] {
			{"c1", "c2"},
			{"c1", "c3"}
		};

		TumbleWindowCrossCategoricalStatFeatures twccFeature =
			new TumbleWindowCrossCategoricalStatFeatures()
				.setWindowTimes("3s")
				.setCategoricalStatistics(ccTypes)
				.setCrossFeatureCols(ccFeatureCols)
				.setGroupCols(l2GroupCols);

		HopWindowCrossCategoricalStatFeatures hwccFeature =
			new HopWindowCrossCategoricalStatFeatures()
				.setWindowTimes("5s")
				.setHopTimes("2s")
				.setCategoricalStatistics(ccTypes)
				.setCrossFeatureCols(ccFeatureCols)
				.setGroupCols(l2GroupCols);

		SessionWindowCrossCategoricalStatFeatures swccFeature =
			new SessionWindowCrossCategoricalStatFeatures()
				.setSessionGapTimes("5s")
				.setCategoricalStatistics(ccTypes)
				.setCrossFeatureCols(ccFeatureCols)
				.setGroupCols(l2GroupCols);

		InterfaceWindowStatFeatures[] features = new InterfaceWindowStatFeatures[] {
			twnFeature,
			hwnFeature,
			swnFeature,
			twcFeature,
			hpcFeature,
			swcFeature,
			twccFeature,
			hwccFeature,
			swccFeature
		};

		BatchOperator <?> gf = new GenerateFeatureOfWindowBatchOp()
			.setTimeCol("ts")
			.setFeatureDefinitions(features);

		source.link(gf).print();
	}

}