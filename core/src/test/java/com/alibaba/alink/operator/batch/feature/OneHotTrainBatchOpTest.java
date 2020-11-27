package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.feature.OneHotModelInfo;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculatorTransformer;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class OneHotTrainBatchOpTest extends AlinkTestBase {
	@Test
	public void transformTest() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("a", 1, 1.1, 1),
				Row.of("b", -2, 0.9, 1),
				Row.of("c", 100, -0.01, 1),
				Row.of("e", -99, 100.9, 2),
				Row.of("a", 1, 1.1, 2),
				Row.of("b", -2, 0.9, 1),
				Row.of("c", 100, -0.01, 2),
				Row.of("d", -99, 100.9, 2),
				Row.of(null, null, 1.1, 1)
			};

		String[] colnames = new String[] {"col1", "col2", "col3", "label"};
		MemSourceBatchOp sourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);

		OneHotTrainBatchOp op = new OneHotTrainBatchOp()
			.setSelectedCols("col1")
			.linkFrom(sourceBatchOp);

		OneHotPredictBatchOp predictBatchOp = new OneHotPredictBatchOp()
			.setOutputCols("output")
			.linkFrom(op, sourceBatchOp);

		Assert.assertEquals(op.count(), 6);
		Assert.assertEquals(op.getSideOutput(0).count(), 1);
		predictBatchOp.collect();
	}

	@Test
	public void testLazyPrint() throws Exception {
		RandomTableSourceBatchOp op = new RandomTableSourceBatchOp()
			.setIdCol("id")
			.setNumCols(40)
			.setNumRows(200L);

		OneHotTrainBatchOp qop = new OneHotTrainBatchOp()
			.setSelectedCols(ArrayUtils.removeElements(op.getColNames(), "id"))
			.linkFrom(op);

		qop.lazyCollectModelInfo(new Consumer <OneHotModelInfo>() {
			@Override
			public void accept(OneHotModelInfo oneHotModelInfo) {
				for (String s : oneHotModelInfo.getSelectedColsInModel()) {
					Assert.assertEquals(oneHotModelInfo.getDistinctTokenNumber(s).intValue(), 200);
				}
			}
		});

		qop.lazyPrintModelInfo();

		BatchOperator.execute();

	}

	@Test
	public void testTransform1() throws Exception {
		Row[] oneHotModels = new Row[] {
			Row.of(-1L, "{\"selectedCols\":\"[\\\"col1\\\"]\",\"selectedColTypes\":\"[\\\"VARCHAR\\\"]\"}", null),
			Row.of(0L, "d", 0L),
			Row.of(0L, "a", 1L),
			Row.of(0L, "e", 2L),
			Row.of(0L, "b", 3L),
			Row.of(0L, "c", 4L)
		};
		BatchOperator source = new MemSourceBatchOp(Arrays.asList(oneHotModels),
			new OneHotModelDataConverter().getModelSchema());
		FeatureBinsCalculator border = (FeatureBinsCalculator) OneHotTrainBatchOp.transformModelToFeatureBins(
			source.getDataSet()).collect().get(0);
		FeatureBinsCalculatorTransformer.toFeatureBins(border);

		Assert.assertEquals(border.getBinCount(), 5);
		Assert.assertEquals(border.getFeatureType(), "STRING");
		Assert.assertEquals((long) border.bin.nullBin.getIndex(), 5L);
		Assert.assertEquals((long) border.bin.elseBin.getIndex(), 6L);
		Assert.assertEquals(border.bin.normBins.size(), 5);
		for (int i = 0; i < 5; i++) {
			Assert.assertEquals((long) border.bin.normBins.get(i).getIndex(), i);
		}
	}

	@Test
	public void testTransform() {
		String s = "[{\"binDivideType\":\"DISCRETE\",\"featureName\":\"col1\",\"bin\":{\"NORM\":[{\"values\":[\"b\"],"
			+ "\"total\":2,\"positive\":2,\"index\":1,\"negative\":0,\"totalRate\":0.2222222222222222,"
			+ "\"positiveRate\":0.4,\"negativeRate\":0.0,\"positivePercentage\":1.0},{\"values\":[\"c\"],"
			+ "\"woe\":-0.2231435513142097,\"total\":2,\"positive\":1,\"index\":2,\"negative\":1,"
			+ "\"totalRate\":0.2222222222222222,\"positiveRate\":0.2,\"negativeRate\":0.25,"
			+ "\"positivePercentage\":0.5,\"iv\":0.011157177565710483},{\"values\":[\"a\"],"
			+ "\"woe\":-0.2231435513142097,\"total\":2,\"positive\":1,\"index\":0,\"negative\":1,"
			+ "\"totalRate\":0.2222222222222222,\"positiveRate\":0.2,\"negativeRate\":0.25,"
			+ "\"positivePercentage\":0.5,\"iv\":0.011157177565710483}],\"NULL\":{\"total\":1,\"positive\":1,"
			+ "\"index\":3,\"negative\":0,\"totalRate\":0.1111111111111111,\"positiveRate\":0.2,\"negativeRate\":0.0,"
			+ "\"positivePercentage\":1.0},\"ELSE\":{\"total\":2,\"positive\":0,\"index\":4,\"negative\":2,"
			+ "\"totalRate\":0.2222222222222222,\"positiveRate\":0.0,\"negativeRate\":0.5,"
			+ "\"positivePercentage\":0.0}},\"featureType\":\"STRING\",\"iv\":0.022314355131420965,\"binCount\":3}]\n";
		List <Tuple2 <Long, FeatureBinsCalculator>> featureList = new ArrayList <>();
		long index = 0L;
		List <String> name = new ArrayList <>();
		for (FeatureBinsCalculator calculator : FeatureBinsUtil.deSerialize(s)) {
			featureList.add(Tuple2.of(index++, calculator));
			name.add(calculator.getFeatureName());
		}
		RowCollector modelRows = new RowCollector();
		modelRows.clear();
		Params meta = new Params().set(HasSelectedCols.SELECTED_COLS, name.toArray(new String[0]));
		OneHotTrainBatchOp.transformFeatureBinsToModel(featureList, modelRows, meta);
		List <Row> list = modelRows.getRows();
		Assert.assertEquals(new OneHotModelDataConverter().load(list).modelData.getNumberOfTokensOfColumn("col1"), 3);
	}

}