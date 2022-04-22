package com.alibaba.alink.common.annotation;

import com.alibaba.alink.operator.batch.outlier.LofOutlierBatchOp;
import com.alibaba.alink.params.outlier.WithMultiVarParams;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.annotation.PublicOperatorUtils.listAlgoOperators;

public class ParamAnnotationUtilsTest {
	@Test
	public void testGetParamSelectColumnSpecsOnClass() {
		List <ParamSelectColumnSpec> specs = ParamAnnotationUtils.getParamSelectColumnSpecs(LofOutlierBatchOp.class);
		Assert.assertEquals(5, specs.size());
	}

	@Test
	public void testGetParamSelectColumnSpecsOnInterface() {
		List <ParamSelectColumnSpec> specs = ParamAnnotationUtils.getParamSelectColumnSpecs(WithMultiVarParams.class);
		Assert.assertEquals(3, specs.size());
	}

	@Test
	public void testGetParamMutexRuleOnClass() {
		List <ParamMutexRule> rules = ParamAnnotationUtils.getParamMutexRules(LofOutlierBatchOp.class);
		Assert.assertEquals(2, rules.size());
	}

	@Test
	public void testGetParamMutexRuleOnInterface() {
		List <ParamMutexRule> rules = ParamAnnotationUtils.getParamMutexRules(WithMultiVarParams.class);
		Assert.assertEquals(2, rules.size());
	}

	@Ignore
	@Test
	public void testCoverage() {
		List <Class <?>> notCovered = new ArrayList <>();

		for (Class <?> operator : listAlgoOperators()) {
			List <ParamSelectColumnSpec> paramSelectColumnSpecs = ParamAnnotationUtils
				.getParamSelectColumnSpecs(operator);

			if (paramSelectColumnSpecs.isEmpty()) {
				notCovered.add(operator);
			}
		}

		if (!notCovered.isEmpty()) {
			System.err.printf("%d operators not covered by @ParamSelectColumnSpec:%n", notCovered.size());

			for (Class <?> element : notCovered) {
				System.err.printf("%s%n", element);
			}
		}
	}


	@Ignore
	@Test
	public void testParamMutexRuleCoverage() {
		List <Class <?>> notCovered = new ArrayList <>();

		for (Class <?> operator : listAlgoOperators()) {
			List <ParamMutexRule> paramMutexRules = ParamAnnotationUtils
				.getParamMutexRules(operator);

			if (paramMutexRules.isEmpty()) {
				notCovered.add(operator);
			}
		}

		if (!notCovered.isEmpty()) {
			System.err.printf("%d operators not covered by @ParamMutexRule:%n", notCovered.size());

			for (Class <?> element : notCovered) {
				System.err.printf("%s%n", element);
			}
		}
	}
}
