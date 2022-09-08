package com.alibaba.alink.common.linalg;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for VectorUtil.
 */

public class VectorUtilTest extends AlinkTestBase {
	@Test
	public void testParseDenseAndToString() {
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		String str = VectorUtil.toString(vec);
		Assert.assertEquals(str, "1.0 2.0 -3.0");
		Assert.assertArrayEquals(vec.getData(), VectorUtil.parseDense(str).getData(), 0);
	}

	@Test
	public void testParseDenseWithSpace() {
		DenseVector vec1 = VectorUtil.parseDense("1 2 -3");
		DenseVector vec2 = VectorUtil.parseDense(" 1  2  -3 ");
		DenseVector vec = new DenseVector(new double[] {1, 2, -3});
		Assert.assertArrayEquals(vec1.getData(), vec.getData(), 0);
		Assert.assertArrayEquals(vec2.getData(), vec.getData(), 0);
	}

	@Test
	public void testSparseToString() {
		SparseVector v1 = new SparseVector(8, new int[] {1, 3, 5, 7}, new double[] {2.0, 2.0, 2.0, 2.0});
		Assert.assertEquals(VectorUtil.toString(v1), "$8$1:2.0 3:2.0 5:2.0 7:2.0");
	}

	@Test
	public void testParseSparse() {
		SparseVector vec1 = VectorUtil.parseSparse("0:1 2:-3");
		SparseVector vec3 = VectorUtil.parseSparse("$4$0:1 2:-3");
		SparseVector vec4 = VectorUtil.parseSparse("$4$");
		SparseVector vec5 = VectorUtil.parseSparse("");
		Assert.assertEquals(vec1.get(0), 1., 0.);
		Assert.assertEquals(vec1.get(2), -3., 0.);
		Assert.assertArrayEquals(vec3.toDenseVector().getData(), new double[] {1, 0, -3, 0}, 0);
		Assert.assertEquals(vec3.size(), 4);
		Assert.assertArrayEquals(vec4.toDenseVector().getData(), new double[] {0, 0, 0, 0}, 0);
		Assert.assertEquals(vec4.size(), 4);
		Assert.assertEquals(vec5.size(), -1);
	}

	@Test
	public void testParseAndToStringOfVector() {
		Vector sparse = VectorUtil.parseSparse("0:1 2:-3");
		Vector dense = VectorUtil.parseDense("1 0 -3");
		Vector vector = VectorUtil.getVector("0:1 2:-3");
		Vector spv = VectorUtil.getSparseVector("0:1 2:-3");

		Assert.assertEquals(VectorUtil.toString(sparse), "0:1.0 2:-3.0");
		Assert.assertEquals(VectorUtil.toString(dense), "1.0 0.0 -3.0");
		Assert.assertTrue(VectorUtil.getVector("$4$0:1 2:-3") instanceof SparseVector);
		Assert.assertTrue(VectorUtil.getVector("1 0 -3") instanceof DenseVector);
		Assert.assertTrue(vector instanceof SparseVector);
		Assert.assertTrue(spv != null);
	}

	@Test
	public void testAbnormalCase() {
		SparseVector vec;
		vec = VectorUtil.parseSparse(" 1,:1.2,  1:2.4, 2:-3", false);
		System.out.println(vec);

		// $$ 前缀不存在 --> 正常解析
		vec = VectorUtil.parseSparse(" 0:1, 1:2.4, 2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);

		// 分隔符是逗号或空格 --> 正常解析
		vec = VectorUtil.parseSparse("0:1,1:2.4,2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);
		vec = VectorUtil.parseSparse("0:1 1:2.4 2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);

		// $ 前后有空格 --> 正常解析
		vec = VectorUtil.parseSparse(" $ 4 $ 0:1, 1: 2.4,2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		vec = VectorUtil.parseSparse(" $4 $ 0:1, 1: 2.4,2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		vec = VectorUtil.parseSparse(" $ 4$ 0:1, 1: 2.4,2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		vec = VectorUtil.parseSparse("$ 4 $0:1, 1: 2.4,2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);

		// 空格作为分割符
		// 多个空格一起作为分割符  --> 正常解析
		vec = VectorUtil.parseSparse("$4$ 0:1      2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, -3, 0}, 0);
		// key 前后有空格  --> 正常解析
		vec = VectorUtil.parseSparse("$4$  0     :1    1: 2.4 2      : -3 ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// value 后有空格  --> 正常解析
		vec = VectorUtil.parseSparse("$4$0:1   1:2.4  2:-3 ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// value 前有空格  --> 正常解析
		vec = VectorUtil.parseSparse("$4$0:      1   1       : 2.4  2      :  -3 ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// 有key 无value的情形  --> 跳过该kv对，其他kv对正常解析
		vec = VectorUtil.parseSparse(" $4$0:    1:2.4    3:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, 0, -3}, 0);

		// 逗号作为分割符
		// key 前后有空格  --> 正常解析
		vec = VectorUtil.parseSparse("$4$   0 : 1,1 : 2.4, 2: -3  ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// value 前后有空格  --> 正常解析
		vec = VectorUtil.parseSparse("$4$0: 1    ,1: 2.4, 2: -3    ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// 逗号前后有空格 --> 正常解析
		vec = VectorUtil.parseSparse(" $4$ 0:1, 1:2.4   ,     2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3, 0}, 0);
		// 逗号之间存在empty字符串或者若干空格 --> 跳过多余逗号，其他正常解析
		vec = VectorUtil.parseSparse("$4$ 0:1, , ,   ,,         , 2:-3, ,");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, -3, 0}, 0);
		// 有key 无value的情形 --> 跳过该kv对，其他kv对正常解析
		vec = VectorUtil.parseSparse(" $4$0 :, 1 :, 2:2, 3:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, 2, -3}, 0);

		// 有value无key的情形 --> 正常解析，跳过该kv对，其他kv对正常解析
		vec = VectorUtil.parseSparse(" $4$  1: 3.4 , :2.4 ,2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 3.4, -3, 0}, 0);
		// key不是一个整数 --> 正常解析，跳过该kv对
		vec = VectorUtil.parseSparse(" $4$  1: 3.4 , 1.4:2.4 ,2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 3.4, -3, 0}, 0);

		// $$ size为空时，size设置为-1，正常解析
		vec = VectorUtil.parseSparse(" $$0:1, 1:2, 3:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2, 0, -3}, 0);

		//多余冒号 --> 重复冒号的kv对跳过，其他kv对正常解析
		vec = VectorUtil.parseSparse(" $4$0:1, 1:2, 2:::2, 3:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2, 0, -3}, 0);
	}

	@Test
	public void emptyElement() {
		SparseVector vec;
		vec = VectorUtil.parseSparse("$4$0:1 ,  , 2:3 , 3:4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$ , 0:1 , 2:3 , 3:4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$0:1 , 2:3 , 3:4 , ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$0:1 , 2:3 ,  ,  ,  ,3:4 , ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$0:1 , : , 2:3 , 3:4", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$0:1   2:3 3:4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 0, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$0:1 : 2:3 3:4", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, 0, 4}, 0);
	}

	@Test
	public void extraSpace() {
		SparseVector vec;
		vec = VectorUtil.parseSparse("$4$ 0 : 1 , 1 : 2 ,  2 : 3 , 3 : 4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$ 0 : 1 1 : 2  2 : 3  3 : 4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2, 3, 4}, 0);

		vec = VectorUtil.parseSparse("$4$ 0 : 1 1 : 2 2 : 3 3 : 4");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2, 3, 4}, 0);
	}

	@Test
	public void newTest() {
		SparseVector vec;
		vec = VectorUtil.parseSparse(" 0:1,11,1:2.4, 2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);

		vec = VectorUtil.parseSparse(" 0:1 11 1:2.4  2:-3");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);

		vec = VectorUtil.parseSparse(" ,0:1, 1:2.4,  2:-3,");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);

		vec = VectorUtil.parseSparse("  , 0:1, 1:2.4,  2:-3, ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);
		vec = VectorUtil.parseSparse(" ,0:1, 1:2.4,  2:-3  , ");
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {1, 2.4, -3}, 0);
	}

	@Test
	public void testKKK() {
		SparseVector vec;
		vec = VectorUtil.parseSparse(" $4$0 : 1 :  3:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, 0, 0}, 0);
		vec = VectorUtil.parseSparse(" 0  : 11 : 2.4 :  22 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, -3}, 0);
		vec = VectorUtil.parseSparse(" 0:1 :11: 1:-2 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, -3}, 0);
		vec = VectorUtil.parseSparse(" 0:1 :11 : 1:-2 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, -3}, 0);
		vec = VectorUtil.parseSparse(" 0:1 :11 : 1 :-2 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, -3}, 0);
		vec = VectorUtil.parseSparse(" 0:1 :11 : 1: -2 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 0, -3}, 0);
		vec = VectorUtil.parseSparse("  0:1 :11 :3 1: 12 -2 2:-3", false);
		Assert.assertArrayEquals(vec.toDenseVector().getData(), new double[] {0, 12, -3}, 0);
	}

	@Test
	public void testPreciseParse() {
		try {
			VectorUtil.parseSparse(" $4$0 : 1 :  3:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Sparse vector parse err.  @  $4$0 : 1 :  3:-3");
		}
		try {
			VectorUtil.parseSparse(" 0  : 11 : 2.4 :  22 2:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Sparse vector parse err.  @  0  : 11 : 2.4 :  22 2:-3");
		}
		try {
			VectorUtil.parseSparse(" 0:1 :11: 1:-2 2:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Sparse vector parse err.  @  0:1 :11: 1:-2 2:-3");
		}
		try {
			VectorUtil.parseSparse("$3 0:1 :11: 1:-2 2:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Statement of dimension should end with '$'."
				+ " Fail to getVector sparse vector from string. $3 0:1 :11: 1:-2 2:-3");
		}
		try {
			VectorUtil.parseSparse("$33$ 0:1 1:-dfasf2 2:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "For input string: \"-dfasf2\" @ $33$ 0:1 1:-dfasf2 2:-3");
		}
		try {
			VectorUtil.parseSparse("$33$ 0:1 164:2 2:-3", true);
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Index out of bound.");
		}
	}
}