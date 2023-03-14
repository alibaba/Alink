package com.alibaba.alink.common.annotation;

import org.apache.flink.util.StringUtils;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NameCnAndEnTest extends AlinkTestBase {
	public void testOpCoverage(List <Class <?>> operators) {
		List <Class <?>> notCoveredCn = new ArrayList <>();
		List <Class <?>> notCoveredEn = new ArrayList <>();
		// NameCn values are used as filenames of documents, and on Windows, it is forbidden to have colons in 
		// filenames.
		List <Class <?>> hasColonCn = new ArrayList <>();
		List <Class <?>> hasColonEn = new ArrayList <>();

		for (Class <?> operator : operators) {
			NameCn nameCn = operator.getAnnotation(NameCn.class);
			NameEn nameEn = operator.getAnnotation(NameEn.class);
			if (operator.getName().endsWith("ModelInfoBatchOp")
				|| operator.getName().endsWith("ModelInfoStreamOp")
				|| operator.getSimpleName().startsWith("Base")
				|| operator.getSimpleName().startsWith("Inner")
				|| operator.getSimpleName().startsWith("Test")
				|| operator.getSimpleName().startsWith("Fake")) {
				continue;
			}
			if (!operator.getName().endsWith("BatchOp") && !operator.getName().endsWith("StreamOp")) {
				continue;
			}

			if (null == nameCn || StringUtils.isNullOrWhitespaceOnly(nameCn.value())) {
				notCoveredCn.add(operator);
			} else if (nameCn.value().contains(":")) {
				hasColonCn.add(operator);
			}

			if (null == nameEn || StringUtils.isNullOrWhitespaceOnly(nameEn.value())) {
				notCoveredEn.add(operator);
			} else if (nameEn.value().contains(":")) {
				hasColonEn.add(operator);
			}
		}

		if (!notCoveredCn.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators not annotated by @NameCn: %s",
					notCoveredCn.size(),
					notCoveredCn.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
				)
			);
		}
		if (!hasColonCn.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators has colon in @NameCn: %s",
					hasColonCn.size(),
					hasColonCn.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
				)
			);
		}
		if (!notCoveredEn.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators not annotated by @NameEn: %s",
					notCoveredEn.size(),
					notCoveredEn.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
				)
			);
		}
		if (!hasColonEn.isEmpty()) {
			throw new RuntimeException(
				String.format("%d operators has colon in @NameEn: %s",
					hasColonEn.size(),
					hasColonEn.stream().map(Class::getSimpleName).collect(Collectors.joining(", "))
				)
			);
		}
	}

	@Test
	public void testAlgoOperatorCoverage() {
		testOpCoverage(PublicOperatorUtils.listAlgoOperators());
	}

	@Test
	public void testPipelineOperatorCoverage() {
		testOpCoverage(PublicOperatorUtils.listPipelineOperators());
	}

}
