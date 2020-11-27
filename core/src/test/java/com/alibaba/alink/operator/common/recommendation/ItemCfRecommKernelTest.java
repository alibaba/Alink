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

import java.lang.reflect.Type;
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
			+ "\\\"d\\\"]\",\"rateCol\":\"\\\"rate\\\"\"}"),
		Row.of(null, 0L, "$4$1:0.8846153846153848 2:0.08770580193070288"),
		Row.of(null, 1L, "$4$0:0.8846153846153848 2:0.4385290096535146 3:0.19611613513818404"),
		Row.of(null, 2L, "$4$0:0.08770580193070288 1:0.4385290096535146 3:0.8944271909999159"),
		Row.of(null, 3L, "$4$1:0.19611613513818404 2:0.8944271909999159")
	};

	private Row[] jaccardModel = new Row[] {
		Row.of(null, null, "{\"itemType\":\"\\\"VARCHAR\\\"\",\"items\":\"[\\\"a\\\",\\\"b\\\",\\\"c\\\","
			+ "\\\"d\\\"]\",\"rateCol\":null}"),
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
	private TableSchema modelSchema = new ItemCfRecommModelDataConverter("user", Types.LONG, "item")
		.getModelSchema();
	private TableSchema dataSchema = new TableSchema(new String[] {"user", "item"},
		new TypeInformation <?>[] {Types.LONG, Types.STRING});

	@Test
	public void rate() {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.RATE);
		kernel.loadModel(cosine);

		Assert.assertEquals(kernel.rate(Row.of(0L, "d")), 2.179, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(1L, "c")), 4.167, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(1L, "d")), 4.0, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(2L, "a")), 1.27, 0.001);
		Assert.assertNull(kernel.rate(Row.of(3L, "a")));

		kernel.loadModel(jaccard);
		Assert.assertEquals(kernel.rate(Row.of(0L, "d")), 0.277, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(1L, "c")), 0.5, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(1L, "d")), 0.166, 0.001);
		Assert.assertEquals(kernel.rate(Row.of(2L, "a")), 0.333, 0.001);
		Assert.assertNull(kernel.rate(Row.of(3L, "a")));
	}

	@Test
	public void recommendItemsPerUser() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.ITEMS_PER_USER);
		kernel.loadModel(cosine);
		String res = kernel.recommendItemsPerUser(Row.of(0L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject(
			res, new String[] {"object"}, new Type[] {String.class}
		).get("object").toArray(new String[0]), new String[] {"a", "d", "b", "c"});
		double[] expect = new double[] {0.94, 0.79, 0.58, 0.46};
		Double[] actual = extractScore(res, "score");
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}

		kernel.loadModel(jaccard);
		res = kernel.recommendItemsPerUser(Row.of(1L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject(
			res, new String[] {"object"}, new Type[] {String.class}
		).get("object").toArray(new String[0]), new String[] {"c", "a", "b", "d"});
		expect = new double[] {0.49, 0.33, 0.33, 0.16};
		actual = extractScore(res, "score");
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
		Assert.assertNull(kernel.recommendItemsPerUser(Row.of(3L, "a")));
	}

	@Test
	public void recommendItemsPerUserExcludeKnown() throws Exception {
		Params params = new Params()
			.set(BaseItemsPerUserRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm")
			.set(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN, true);

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.ITEMS_PER_USER);
		kernel.loadModel(cosine);
		String res = kernel.recommendItemsPerUser(Row.of(1L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject(
			res, new String[] {"object"}, new Type[] {String.class}
		).get("object").toArray(new String[0]), new String[] {"c", "d"});
		double[] expect = new double[] {1.09, 0.39};
		Double[] actual = extractScore(res, "score");
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
	}

	@Test
	public void recommendSimilarItems() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_ITEMS);
		kernel.loadModel(cosine);
		String res = kernel.recommendSimilarItems(Row.of(0L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject(
			res, new String[] {"object"}, new Type[] {String.class}
		).get("object").toArray(new String[0]), new String[] {"c", "b"});
		double[] expect = new double[] {0.89, 0.19};
		Double[] actual = extractScore(res, "similarities");
		Assert.assertEquals(expect.length, actual.length);
		for (int i = 0; i < expect.length; i++) {
			Assert.assertEquals(expect[i], actual[i], 0.01);
		}
		Assert.assertNull(kernel.recommendSimilarItems(Row.of(3L, "f")));

	}

	@Test
	public void recommendUsersTest() throws Exception {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.USERS_PER_ITEM);
		kernel.loadModel(cosine);
		String res = kernel.recommendUsersPerItem(Row.of(0L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject((res), new String[] {"object"}, new Type[] {Long.class}
		).get("object").toArray(new Long[0]), new Long[] {2L, 0L, 1L});

		params.set(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN, true);

		kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.USERS_PER_ITEM);
		kernel.loadModel(cosine);
		res = kernel.recommendUsersPerItem(Row.of(0L, "d"));
		Assert.assertArrayEquals(KObjectUtil.deserializeKObject((res), new String[] {"object"}, new Type[] {Long.class}
		).get("object").toArray(new Long[0]), new Long[] {0L, 1L});

		Assert.assertNull(kernel.recommendUsersPerItem(Row.of(3L, "f")));

	}

	@Test
	public void testException1() {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		thrown.expect(RuntimeException.class);
		ItemCfRecommKernel kernel = new ItemCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_USERS);
	}

	@Test
	public void testException2() {
		Params params = new Params()
			.set(BaseRateRecommParams.USER_COL, "user")
			.set(BaseRateRecommParams.ITEM_COL, "item")
			.set(BaseRateRecommParams.RECOMM_COL, "recomm");

		thrown.expect(RuntimeException.class);
		UserCfRecommKernel kernel = new UserCfRecommKernel(modelSchema, dataSchema, params, RecommType.SIMILAR_ITEMS);
	}

	public static Double[] extractScore(String res, String key) {
		return KObjectUtil.deserializeKObject(
			res, new String[] {key}, new Type[] {Double.class}
		).get(key).toArray(new Double[0]);
	}
}