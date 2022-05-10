package com.alibaba.alink.common.annotation;

import org.apache.flink.util.StringUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NameCnTest {
	public void testCoverage(List <Class <?>> operators) {
		List <Class <?>> notCovered = new ArrayList <>();
		// NameCn values are used as filenames of documents, and on Windows, it is forbidden to have colons in filenames.
		List <Class <?>> hasColon = new ArrayList <>();

		for (Class <?> operator : operators) {
			NameCn nameCn = operator.getAnnotation(NameCn.class);
			if (null == nameCn || StringUtils.isNullOrWhitespaceOnly(nameCn.value())) {
				notCovered.add(operator);
			} else if (nameCn.value().contains(":")) {
				hasColon.add(operator);
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
	public void testAlgoOperatorCoverage() {
		testCoverage(PublicOperatorUtils.listAlgoOperators());
	}

	@Test
	public void testPipelineOperatorCoverage() {
		testCoverage(PublicOperatorUtils.listPipelineOperators());
	}
}
