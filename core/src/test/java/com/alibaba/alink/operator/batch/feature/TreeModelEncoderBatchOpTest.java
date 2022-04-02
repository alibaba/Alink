package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelDataConverter;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.TreeModelEncoderParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TreeModelEncoderBatchOpTest extends AlinkTestBase {

	public static Row[] modelData = new Row[] {
		Row.of(0L, "{\"numTrees\":\"1\",\"minSamplesPerLeaf\":\"1\",\"featureCols\":\"[\\\"col0\\\",\\\"col1\\\"]\"," +
			"\"gbdt.y.period\":\"0.0\",\"criteriaType\":\"\\\"PAI\\\"\","
			+ "\"updaterType\":\"\\\"NEWTON_SINGLE_STEP_UPDATER\\\"\","
			+
			"\"maxLeaves\":\"3\",\"learningRate\":\"1.0\",\"lossType\":\"\\\"LOG_LOSS\\\"\"," +
			"\"treePartition\":\"{\\\"partitions\\\":[{\\\"f0\\\":0,\\\"f1\\\":5}]}\"," +
			"\"stringIndexerModelPartition\":\"{\\\"f0\\\":0,\\\"f1\\\":0}\"," +
			"\"algoType\":\"1\",\"boosterType\":\"\\\"HESSION_BASE\\\"\"," +
			"\"categoricalCols\":\"[]\",\"featureTypes\":\"[\\\"INT\\\",\\\"INT\\\"]\"," +
			"\"labelCol\":\"\\\"label\\\"\",\"labelTypeName\":\"\\\"INT\\\"\"}", null),
		Row.of(1048576L,
			"{\"node\":{\"featureIndex\":1,\"gain\":0.41666666666666663,\"counter\":{\"weightSum\":10.0,"
				+ "\"numInst\":10,\"distributions\":[0.0,2.5]},\"categoricalSplit\":null,\"continuousSplit\":2.0,"
				+ "\"missingSplit\":[1]},\"id\":0,\"nextIds\":[1,2]}",
			null),
		Row.of(2097152L,
			"{\"node\":{\"featureIndex\":0,\"gain\":0.75,\"counter\":{\"weightSum\":4.0,\"numInst\":4,"
				+ "\"distributions\":[-1.0,1.0]},\"categoricalSplit\":null,\"continuousSplit\":1.0,"
				+ "\"missingSplit\":[1]},\"id\":1,\"nextIds\":[3,4]}",
			null),
		Row.of(3145728L,
			"{\"node\":{\"featureIndex\":-1,\"gain\":0.0,\"counter\":{\"weightSum\":6.0,\"numInst\":6,"
				+ "\"distributions\":[0.6666666666666666,1.5]},\"categoricalSplit\":null,\"continuousSplit\":0.0,"
				+ "\"missingSplit\":[1]},\"id\":2,\"nextIds\":null}",
			null),
		Row.of(4194304L,
			"{\"node\":{\"featureIndex\":-1,\"gain\":0.0,\"counter\":{\"weightSum\":3.0,\"numInst\":3,"
				+ "\"distributions\":[-2.0,0.75]},\"categoricalSplit\":null,\"continuousSplit\":0.0,"
				+ "\"missingSplit\":null},\"id\":3,\"nextIds\":null}",
			null),
		Row.of(5242880L,
			"{\"node\":{\"featureIndex\":-1,\"gain\":0.0,\"counter\":{\"weightSum\":1.0,\"numInst\":1,"
				+ "\"distributions\":[2.0,0.25]},\"categoricalSplit\":null,\"continuousSplit\":0.0,"
				+ "\"missingSplit\":null},\"id\":4,\"nextIds\":null}",
			null),
		Row.of(2251799812636672L, null, 0),
		Row.of(2251799812636673L, null, 1),
	};

	@Test
	public void testEncoder() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		TreeModelEncoderModelMapper modelMapper = new TreeModelEncoderModelMapper(
			new TreeModelDataConverter(memSourceBatchOp.getColTypes()[2]).getModelSchema(),
			memSourceBatchOp.getSchema(),
			new Params().set(TreeModelEncoderParams.PREDICTION_COL, "predictition")
		);

		modelMapper.loadModel(Arrays.asList(modelData));
		Row result = modelMapper.map(Row.of(1, null, 0));

		Assert.assertEquals(4, result.getArity());
		Assert.assertEquals(1, result.getField(0));
		Assert.assertNull(result.getField(1));
		Assert.assertEquals(0, result.getField(2));
		Assert.assertEquals("$3$2:1.0", VectorUtil.serialize(result.getField(3)));
	}
}