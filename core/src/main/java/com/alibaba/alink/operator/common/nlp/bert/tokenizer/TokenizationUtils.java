package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TokenizationUtils {

	private static final Set <Byte> CONTROL_CATEGORY_SET = new HashSet <>(
		Arrays.asList(Character.CONTROL, Character.FORMAT, Character.PRIVATE_USE, Character.SURROGATE,
			Character.UNASSIGNED));

	private static final Set <Byte> PUNCTUATION_CATEGORY_SET = new HashSet <>(
		Arrays.asList(Character.CONNECTOR_PUNCTUATION, Character.DASH_PUNCTUATION,
			Character.END_PUNCTUATION, Character.FINAL_QUOTE_PUNCTUATION, Character.INITIAL_QUOTE_PUNCTUATION,
			Character.OTHER_PUNCTUATION, Character.START_PUNCTUATION));

	public static boolean isControl(int cp) {
		if (cp == '\t' || cp == '\n' || cp == '\r') {
			return false;
		}
		int type = Character.getType(cp);
		return CONTROL_CATEGORY_SET.contains((byte) type);
	}

	public static boolean isWhitespace(int cp) {
		if (cp == ' ' || cp == '\t' || cp == '\n' || cp == '\r') {
			return true;
		}
		int type = Character.getType(cp);
		return Character.SPACE_SEPARATOR == type;
	}

	/**
	 * Checks whether `char` is a punctuation character.
	 * <p>
	 * We treat all non-letter/number ASCII as punctuation. Characters such as "^", "$", && "`" are not in the Unicode
	 * Punctuation class but we treat them as punctuation anyways, f|| consistency.
	 *
	 * @param cp codepoint
	 * @return
	 */
	public static boolean isPunctuation(int cp) {
		if ((cp >= 33 && cp <= 47) || (cp >= 58 && cp <= 64) || (cp >= 91 && cp <= 96) || (cp >= 123 && cp <= 126)) {
			return true;
		}
		int type = Character.getType(cp);
		return PUNCTUATION_CATEGORY_SET.contains((byte) type);
	}

	public static String[] whitespaceTokenize(String text) {
		return text.trim().split("\\s");
	}
}
