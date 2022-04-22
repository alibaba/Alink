package com.alibaba.alink.common.annotation;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.classification.BertTextPairClassifierTrainBatchOp;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PortAnnotationUtilsTest {

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

	@Ignore
	@Test
	public void testCoverage() {
		List <Class <?>> notCovered = new ArrayList <>();
		for (Class <?> operator : PublicOperatorUtils.listAlgoOperators()) {
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
