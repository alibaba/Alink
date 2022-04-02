package com.alibaba.alink.common.annotation;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.BertTextPairClassifierTrainBatchOp;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PortAnnotationUtilsTest {

	final static String BASE_PKG_NAME = "com.alibaba.alink";

	final static List <Class <?>> BASES = Arrays.asList(BatchOperator.class, StreamOperator.class);
	final static List <Class <? extends Annotation>> PORT_ANNOTATIONS =
		Arrays.asList(InputPorts.class, OutputPorts.class);

	@Test
	public void printAnnotations() {
		Class <?> target = BertTextPairClassifierTrainBatchOp.class;

		Tuple2 <InputPorts, OutputPorts> portsDesc = PortAnnotationUtils.getInputOutputPorts(target);

		System.out.printf("InputPorts:%n");

		for (PortSpec portSpec : portsDesc.f0.values()) {
			System.out.printf("%s%n", portSpec);
		}

		System.out.printf("OutputPorts:%n");

		for (PortSpec portSpec : portsDesc.f1.values()) {
			System.out.printf("%s%n", portSpec);
		}

		List <ParamSelectColumnSpec> paramSelectColumnSpecs = ParamAnnotationUtils
			.getParamSelectColumnSpecs(target);

		System.out.printf("Paramspes:%n");

		for (ParamSelectColumnSpec paramSelectColumnSpec : paramSelectColumnSpecs) {
			System.out.printf("%s%n", paramSelectColumnSpec);
		}

		List <ParamMutexRule> paramMutexRules = ParamAnnotationUtils
			.getParamMutexRules(target);

		System.out.printf("ParamMutexRules:%n");

		for (ParamMutexRule paramMutexRule : paramMutexRules) {
			System.out.printf("%s%n", paramMutexRule);
		}
	}

	@Test
	public void testGetInputOutputPorts() {
		Tuple2 <InputPorts, OutputPorts> inputOutputPorts =
			PortAnnotationUtils.getInputOutputPorts(BaseOutlierBatchOp.class);
		Assert.assertNotNull(inputOutputPorts);
		InputPorts inputPorts = inputOutputPorts.f0;
		Assert.assertEquals(1, inputPorts.values().length);
		Assert.assertEquals(PortType.DATA, inputPorts.values()[0].value());

		OutputPorts outputPorts = inputOutputPorts.f1;
		Assert.assertEquals(1, outputPorts.values().length);
		Assert.assertEquals(PortType.DATA, outputPorts.values()[0].value());
	}

	public static List<Class <?>> getAllOperator() {
		Reflections ref = new Reflections(BASE_PKG_NAME);
		List <Class <?>> operators = new ArrayList <>();
		for (Class <?> base : BASES) {
			operators.addAll(ref.getSubTypesOf(base));
		}

		return operators
			.stream()
			.filter(aClass -> !(
				!Modifier.isPublic(aClass.getModifiers())
					|| aClass.getEnclosingClass() != null
					|| Modifier.isAbstract(aClass.getModifiers())
					|| ExtractModelInfoBatchOp.class.isAssignableFrom(aClass)
					|| Arrays.stream(aClass.getAnnotations()).anyMatch(
						annotation -> annotation.annotationType().equals(Internal.class))
			))
			.sorted(Comparator.comparing(Class::toString))
			.collect(Collectors.toList());
	}

	@Ignore
	@Test
	public void testCoverage() {
		List <Class <?>> notCovered = new ArrayList <>();
		for (Class <?> operator : getAllOperator()) {
			Tuple2 <InputPorts, OutputPorts> inputOutputPorts = PortAnnotationUtils.getInputOutputPorts(operator);
			if (null == inputOutputPorts) {
				notCovered.add(operator);
			}
		}
		Assert.assertTrue(String.format("These operators are not annotated by @InputPorts/@OutputPorts: %s",
				notCovered.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))),
			notCovered.isEmpty());
	}
}
