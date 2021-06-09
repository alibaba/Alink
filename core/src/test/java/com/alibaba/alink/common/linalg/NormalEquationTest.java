package com.alibaba.alink.common.linalg;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class NormalEquationTest extends AlinkTestBase {
	@Test
	public void solve() throws Exception {
		NormalEquation ls = new NormalEquation(4);
		DenseVector x = new DenseVector(4);
		DenseVector buffer = DenseVector.rand(4);

		ls.add(buffer, 1.0, 1.0);
		ls.regularize(0.1);
		ls.solve(x, true);

		ls.reset();
		ls.add(buffer, 1.0, 1.0);
		ls.regularize(0.1);
		ls.merge(DenseMatrix.eye(4));
		ls.solve(x, false);
	}

}