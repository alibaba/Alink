package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

/**
 * Unit test for ItemKnnRecommKernel.
 */

public class ItemCfRecommKernelTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private Row[] cosineModel = new Row[] {
		Row.of(0L, null, "$4$0:1.0 1:3.0 2:2.0"),
		Row.of(1L, null, "$4$0:5.0 1:4.0"),
		Row.of(2L, null, "$4$1:1.0 2:4.0 3:3.0"),
		Row.of(null, null, "{\"itemType\":\"\\\"VARCHAR\\\"\",\"items\":\"[\\\"a\\\",\\\"b\\\",\\\"c\\\","
			+ "\\\"d\\\"]\",\"rateCol\":\"\\\"rate\\\"\",\"userCol\":\"\\\"user_id\\\"\",\"itemCol\":\"\\\"item_id\\\"\"}"),
		Row.of(null, 0L, "$4$1:0.8846153846153848 2:0.08770580193070288"),
		Row.of(null, 1L, "$4$0:0.8846153846153848 2:0.4385290096535146 3:0.19611613513818404"),
		Row.of(null, 2L, "$4$0:0.08770580193070288 1:0.4385290096535146 3:0.8944271909999159"),
		Row.of(null, 3L, "$4$1:0.19611613513818404 2:0.8944271909999159")
	};

	private Row[] jaccardModel = new Row[] {
		Row.of(null, null, "{\"itemType\":\"\\\"VARCHAR\\\"\",\"items\":\"[\\\"a\\\",\\\"b\\\",\\\"c\\\","
			+ "\\\"d\\\"]\",\"rateCol\":null,\"userCol\":\"\\\"user_id\\\"\",\"itemCol\":\"\\\"item_id\\\"\"}"),
		Row.of(0L, null, "$4$0:1.0 1:1.0 2:1.0"),
		Row.of(1L, null, "$4$0:1.0 1:1.0"),
		Row.of(2L, null, "$4$1:1.0 2:1.0 3:1.0"),
		Row.of(null, 0L, "$4$1:0.6666666666666666 2:0.33333333333333326"),
		Row.of(null, 1L, "$4$0:0.6666666666666666 2:0.6666666666666666 3:0.33333333333333326"),
		Row.of(null, 2L, "$4$0:0.33333333333333326 1:0.6666666666666666 3:0.5"),
		Row.of(null, 3L, "$4$1:0.33333333333333326 2:0.5")
	};

	private List <Row> cosine = Arrays.asList(cosineModel);
	private List <Row> jaccard = Arrays.asList(jaccardModel);
	private TableSchema modelSchema = new ItemCfRecommModelDataConverter("user_id", Types.LONG, "item_id")
		.getModelSchema();
	private TableSchema dataSchema = new TableSchema(new String[] {"user_id", "item_id"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	@Test
	public void rate() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.RATE);
		kernel.loadModel(cosine);

		Assert.assertEquals(kernel.rate(new Object[]{0L, "d"}), 2.179, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{1L, "c"}), 4.167, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{1L, "d"}), 4.0, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{2L, "a"}), 1.27, 0.001);
		Assert.assertNull(kernel.rate(new Object[]{3L, "a"}));

		kernel.loadModel(jaccard);
		Assert.assertEquals(kernel.rate(new Object[]{0L, "d"}), 0.277, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{1L, "c"}), 0.5, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{1L, "d"}), 0.166, 0.001);
		Assert.assertEquals(kernel.rate(new Object[]{2L, "a"}), 0.333, 0.001);
		Assert.assertNull(kernel.rate(new Object[]{3L, "a"}));
	}

	@Test
	public void recommendItemsPerUser() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.ITEMS_PER_USER);
		kernel.loadModel(cosine);
		List<Row> rows = kernel.recommendItemsPerUser(0L).getRows();
		String[] items = new String[rows.size()];
		double[] actual = new double[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (String) rows.get(i).getField(0);
			actual[i] = (double) rows.get(i).getField(1);
		}
		Assert.assertArrayEquals(items, new String[] {"a", "d", "b", "c"});
		double[] expect = new double[] {0.94, 0.79, 0.58, 0.46};
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}

		kernel.loadModel(jaccard);
		rows = kernel.recommendItemsPerUser(1L).getRows();
		items = new String[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (String) rows.get(i).getField(0);
			actual[i] = (double) rows.get(i).getField(1);
		}
		Assert.assertArrayEquals(items, new String[] {"c", "a", "b", "d"});
		expect = new double[] {0.49, 0.33, 0.33, 0.16};
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
		Assert.assertNull(kernel.recommendItemsPerUser(3L));
	}

	@Test
	public void recommendItemsPerUserExcludeKnown() throws Exception {
		Params params = new Params()
			.set(BaseItemsPerUserRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm")
			.set(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN, true);

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.ITEMS_PER_USER);
		kernel.loadModel(cosine);
		List<Row> rows = kernel.recommendItemsPerUser(1L).getRows();
		String[] items = new String[rows.size()];
		double[] actual = new double[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (String) rows.get(i).getField(0);
			actual[i] = (double) rows.get(i).getField(1);
		}
		Assert.assertArrayEquals(items, new String[] {"c", "d"});
		double[] expect = new double[] {1.09, 0.39};
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
	}

	@Test
	public void recommendSimilarItems() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_ITEMS);
		kernel.loadModel(cosine);
		List<Row> rows = kernel.recommendSimilarItems("d").getRows();
		String[] items = new String[rows.size()];
		double[] actual = new double[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (String) rows.get(i).getField(0);
			actual[i] = (double) rows.get(i).getField(1);
		}
		Assert.assertArrayEquals(items, new String[] {"c", "b"});
		double[] expect = new double[] {0.89, 0.19};
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
		Assert.assertNull(kernel.recommendSimilarItems("f"));

	}

	@Test
	public void recommendUsersTest() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.USERS_PER_ITEM);
		kernel.loadModel(cosine);
		List<Row> rows = kernel.recommendUsersPerItem("d").getRows();
		long[] items = new long[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (long) rows.get(i).getField(0);
		}
		Assert.assertArrayEquals(items, new long[] {2L, 0L, 1L});

		params.set(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN, true);

		kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.USERS_PER_ITEM);
		kernel.loadModel(cosine);
		rows = kernel.recommendUsersPerItem("d").getRows();
		items = new long[rows.size()];
		for (int i = 0; i < rows.size(); ++i) {
			items[i] = (long) rows.get(i).getField(0);
		}
		Assert.assertArrayEquals(items, new long[] {0L, 1L});

		Assert.assertNull(kernel.recommendUsersPerItem("f"));

	}

	@Test
	public void testException1() {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		thrown.expect(RuntimeException.class);
		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_USERS);
	}

	@Test
	public void testException2() {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user_id")
			.set(BaseRateRecommParams.ITEM_COL, "item_id")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		thrown.expect(RuntimeException.class);
		UserCfRecommKernel kernel = new UserCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_ITEMS);
	}
}