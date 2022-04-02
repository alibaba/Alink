package com.alibaba.alink.common.annotation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class NameCnTest {
	@Test
	public void testOperatorCoverage() {
		List <Class <?>> notCovered = new ArrayList <>();
		// NameCn values are used as filenames of documents, and on Windows, it is forbidden to have colons in filenames.
		List <Class <?>> hasColon = new ArrayList <>();

		for (Class <?> operator : PortAnnotationUtilsTest.getAllOperator()) {
			NameCn nameCn = operator.getAnnotation(NameCn.class);
			if (null == nameCn || null == nameCn.value()) {
				notCovered.add(operator);
			} else if (nameCn.value().contains(":")) {
				hasColon.add(operator);
			}
		}

		if (!notCovered.isEmpty()) {
			System.err.printf("%d operators not covered by @NameCn:%n", notCovered.size());
			for (Class <?> element : notCovered) {
				System.err.printf("%s%n", element);
			}
		}
		if (!hasColon.isEmpty()) {
			System.err.printf("%d operators has colon in @NameCn:%n", hasColon.size());
			for (Class <?> element : hasColon) {
				System.err.printf("%s%n", element);
			}
		}
	}
}
