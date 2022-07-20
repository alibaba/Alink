package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TokenizerUtils {
	public static int[] nCopiesArray(int v, int len) {
		return createArrayWithCopies(v, len);
	}

	/**
	 * Providing `specs` has values: `v0, len0, v1, len1, ...`, the returned array is equivalent to this Python
	 * expression: `[v0] * len0 + [v1] * len1 + ...`
	 * <p>
	 * The total length of returned array is not expected to exceed integer limit.
	 *
	 * @param specs
	 * @return
	 */
	public static int[] createArrayWithCopies(int... specs) {
		AkPreconditions.checkArgument(specs.length % 2 == 0,
			"Argument `specs` should have values like : `v0, len0, v1, len1, ...`");
		int total = 0;
		for (int i = 1; i < specs.length; i += 2) {
			total += specs[i];
		}
		int[] arr = new int[total];
		int start = 0;
		for (int i = 0; i < specs.length; i += 2) {
			int v = specs[i];
			int len = specs[i + 1];
			Arrays.fill(arr, start, start + len, v);
			start += len;
		}
		return arr;
	}

	public static String readFileToString(File file) {
		try {
			return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(String.format("Cannot read file %s", file.getAbsolutePath()));
		}
	}
}
