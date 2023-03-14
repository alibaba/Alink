package com.alibaba.alink.operator.local.feature;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.feature.FeatureBundles;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.pipeline.feature.ExclusiveFeatureBundle;
import junit.framework.TestCase;
import org.junit.Assert;

public class ExclusiveFeatureBundleTrainLocalOpTest extends TestCase {

	public void testExtract() {
		SparseVector[] vectors = new SparseVector[] {
			VectorUtil.parseSparse("$7$0:-3 2:1 4:1"),
			VectorUtil.parseSparse("$7$0:3 1:1 5:1"),
			VectorUtil.parseSparse("$7$0:1 3:1 5:1"),
		};

		FeatureBundles bundles = ExclusiveFeatureBundleTrainLocalOp.extract(vectors);

		Assert.assertEquals(bundles.getNumFeatures(), 3);

		MemSourceLocalOp source = new MemSourceLocalOp(vectors, "vec");

		LocalOperator <?> model = source
			.link(
				new ExclusiveFeatureBundleTrainLocalOp()
					.setSparseVectorCol("vec")
			);

		model.link(new ExclusiveFeatureBundleModelInfoLocalOp()).lazyPrintModelInfo();

		new ExclusiveFeatureBundlePredictLocalOp()
			.setSparseVectorCol("vec")
			.setReservedCols()
			.linkFrom(model, source)
			.print();

		new ExclusiveFeatureBundle()
			.setSparseVectorCol("vec")
			.setReservedCols()
			.fit(source)
			.transform(source)
			.print();
	}
}