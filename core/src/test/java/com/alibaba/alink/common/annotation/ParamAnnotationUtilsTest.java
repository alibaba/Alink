package com.alibaba.alink.common.annotation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.operator.batch.outlier.LofOutlierBatchOp;
import com.alibaba.alink.params.outlier.WithMultiVarParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.alink.common.annotation.PublicOperatorUtils.listAlgoOperators;

public class ParamAnnotationUtilsTest extends AlinkTestBase {

	public void testParamCoverage(List <Class <?>> operators) {
		Set <Class <?>> notHasCn = new HashSet <>();
		Set <Class <?>> notHasDesc = new HashSet <>();

		for (Class <?> operator : operators) {
			//NameCn nameCn = operator.getAnnotation(NameCn.class);
			Field[] fs = operator.getFields();
			for (Field f : fs) {
				if (!ParamInfo.class.equals(f.getType())) {
					continue;
				}
				NameCn nameCn = f.getAnnotation(NameCn.class);
				if (!f.getDeclaringClass().getName().startsWith("com.alibaba.alink.params")) {
					continue;
				}
				if (null == nameCn || StringUtils.isNullOrWhitespaceOnly(nameCn.value())) {
					notHasCn.add(operator);
					System.out.println(operator.getSimpleName() + ": " + f.getName());
				}

				DescCn descCn = f.getAnnotation(DescCn.class);
				if (null == descCn || StringUtils.isNullOrWhitespaceOnly(descCn.value())) {
					notHasDesc.add(operator);
					System.out.println(operator.getSimpleName() + ": " + f.getName());
				}
			}
		}

		if (!notHasCn.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators not annotated by @NameCn: %s",
					notHasCn.size(),
					notHasCn.stream().map(Class::getSimpleName).collect(Collectors.joining(", \n"))
				)
			);
		}

		if (!notHasDesc.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators not annotated by @DescCn: %s",
					notHasDesc.size(),
					notHasDesc.stream().map(Class::getSimpleName).collect(Collectors.joining(", \n"))
				)
			);
		}
	}

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

	@Test
	public void testOpParamsCoverage() {
		testParamCoverage(PublicOperatorUtils.listAlgoOperators());
	}
}
