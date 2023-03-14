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
		Set <Class <?>> notCovered = new HashSet <>();
		// On Windows, it is forbidden to have colons in filenames.
		List <Class <?>> hasColon = new ArrayList <>();

		for (Class <?> operator : operators) {
			//NameCn nameCn = operator.getAnnotation(NameCn.class);
			Field[] fs = operator.getFields();
			for (Field f : fs) {
				if (!ParamInfo.class.equals(f.getType())) {
					continue;
				}
				NameCn nameCn = f.getAnnotation(NameCn.class);
				if (null == nameCn || StringUtils.isNullOrWhitespaceOnly(nameCn.value())) {
					notCovered.add(operator);
				} else if (nameCn.value().contains(":")) {
					hasColon.add(operator);
				}
			}
		}

		if (!notCovered.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators not annotated by @NameCn: %s",
					notCovered.size(),
					notCovered.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
				)
			);
		}
		if (!hasColon.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators has colon in @NameCn: %s",
					hasColon.size(),
					hasColon.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
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
	public void testParamsCoverage() {
		testParamCoverage(ParamAnnotationUtils.listParamInfos());
	}
}
