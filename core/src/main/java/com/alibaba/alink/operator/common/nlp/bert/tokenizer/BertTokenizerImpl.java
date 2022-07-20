package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isControl;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isPunctuation;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isWhitespace;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.whitespaceTokenize;

public class BertTokenizerImpl extends PreTrainedTokenizer {

	Map <String, Integer> vocab;
	Map <Integer, String> idsToTokens;
	boolean doBasicTokenize;
	BasicTokenizer basicTokenizer;
	WordpieceTokenizer wordpieceTokenizer;

	public static BertTokenizerImpl fromPretrained(String localModelPath) {
		return BertTokenizerImpl.fromPretrained(localModelPath, Kwargs.empty());
	}

	public static BertTokenizerImpl fromPretrained(String localModelPath, Kwargs kwargs) {
		kwargs = kwargs.clone();
		kwargs.put("pretrained_model_name_or_path", localModelPath);
		return PreTrainedTokenizer.fromPretrained(BertTokenizerImpl.class, kwargs);
	}

	/**
	 * Construct a BERT tokenizer, based on WordPiece tokenizer.
	 *
	 * @param vocabFile            File containing the vocabulary.
	 * @param doLowerCase          Whether or not to lowercase the input when tokenizing.
	 * @param doBasicTokenize      Whether or not to do basic tokenization before WordPiece.
	 * @param neverSplit           Collection of tokens which will never be split during tokenization. Only has an
	 *                             effect when doBasicTokenize = True
	 * @param unkToken             The unknown token. A token that is not in the vocabulary cannot be converted to
	 *                                an ID
	 *                             and is set to be this token instead.
	 * @param sepToken             The separator token, which is used when building a sequence from multiple sequences,
	 *                             e.g. two sequences for sequence classification or for a text and a question for
	 *                             question answering. It is also used as the last token of a sequence built with
	 *                             special tokens.
	 * @param padToken             The token used for padding, for example when batching sequences of different
	 *                             lengths.
	 * @param clsToken             The classifier token which is used when doing sequence classification
	 *                                (classification
	 *                             of the whole sequence instead of per-token classification). It is the first token of
	 *                             the sequence when built with special tokens.
	 * @param maskToken            The token used for masking values. This is the token used when training this model
	 *                             with masked language modeling. This is the token which the model will try to
	 *                             predict.
	 * @param tokenizeChineseChars Whether or not to tokenize Chinese characters. This should likely be deactivated for
	 *                             Japanese (see this `issue <https://github
	 *                             .com/huggingface/transformers/issues/328>`__).
	 * @param stripAccents         Whether or not to strip all accents. If this option is not specified, then it
	 *                                will be
	 *                             determined by the value for :obj:`lowercase` (as in the original BERT).
	 */
	public BertTokenizerImpl(Kwargs kwargs) {
		this.config = kwargs;

		File vocabFile = new File((String) (kwargs.get("vocab_file")));
		boolean doBasicTokenize = (Boolean) kwargs.getOrDefault("do_basic_tokenize", true);
		boolean doLowerCase = (Boolean) kwargs.getOrDefault("do_lower_case", true);
		@SuppressWarnings("unchecked")
		Set <String> neverSplit = (Set <String>) kwargs.get("never_split");
		boolean tokenizeChineseChars = (Boolean) kwargs.get("tokenize_chinese_chars");
		Boolean stripAccents = (Boolean) kwargs.get("strip_accents");
		String unkToken = (String) kwargs.get("unk_token");

		this.vocab = loadVocab(vocabFile);
		this.idsToTokens = new LinkedHashMap <>();
		this.vocab.forEach((k, v) -> this.idsToTokens.put(v, k));
		this.doBasicTokenize = doBasicTokenize;
		if (doBasicTokenize) {
			this.basicTokenizer = new BasicTokenizer(doLowerCase, neverSplit, tokenizeChineseChars, stripAccents);
		}
		this.wordpieceTokenizer = new WordpieceTokenizer(vocab, unkToken);
	}

	static LinkedHashMap <String, Integer> loadVocab(File vocabFile) {
		List <String> lines;
		try {
			lines = FileUtils.readLines(vocabFile, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new AkUnclassifiedErrorException(
				String.format("Cannot read all lines in %s", vocabFile.getAbsoluteFile()));
		}
		int index = 0;
		LinkedHashMap <String, Integer> vocab = new LinkedHashMap <>();
		for (String line : lines) {
			vocab.put(line, index);
			index += 1;
		}
		return vocab;
	}

	@Override
	protected int convertTokenToIdImpl(String d) {
		return vocab.getOrDefault(d, vocab.get(specialTokenValues.get(SPECIAL_TOKENS.UNK_TOKEN)));
	}

	@Override
	int vocabSize() {
		return vocab.size();
	}

	@Override
	protected String[] tokenizeImpl(String text) {
		List <String> splitTokens = new ArrayList <>();
		if (doBasicTokenize) {
			for (String token : basicTokenizer.tokenize(text, Collections.emptySet())) {    // TODO: all_special_tokens
				if (basicTokenizer.neverSplit.contains(token)) {
					splitTokens.add(token);
				} else {
					splitTokens.addAll(Arrays.asList(wordpieceTokenizer.tokenizer(token)));
				}
			}
		} else {
			splitTokens = Arrays.asList(wordpieceTokenizer.tokenizer(text));
		}
		return splitTokens.toArray(new String[0]);
	}

	public int[] buildInputsWithSpecialTokens(int[] tokenIds0) {
		return buildInputsWithSpecialTokens(tokenIds0, null);
	}

	@Override
	public int[] buildInputsWithSpecialTokens(int[] tokenIds0, int[] tokenIds1) {
		int clsTokenId = convertTokenToIdImpl(specialTokenValues.get(SPECIAL_TOKENS.CLS_TOKEN));
		int sepTokenId = convertTokenToIdImpl(specialTokenValues.get(SPECIAL_TOKENS.SEP_TOKEN));
		int[] arr;
		if (null == tokenIds1) {
			arr = new int[tokenIds0.length + 2];
			arr[0] = clsTokenId;
			System.arraycopy(tokenIds0, 0, arr, 1, tokenIds0.length);
			arr[tokenIds0.length + 1] = sepTokenId;
		} else {
			arr = new int[tokenIds0.length + tokenIds1.length + 3];
			arr[0] = clsTokenId;
			System.arraycopy(tokenIds0, 0, arr, 1, tokenIds0.length);
			arr[tokenIds0.length + 1] = sepTokenId;
			System.arraycopy(tokenIds1, 0, arr, tokenIds0.length + 2, tokenIds1.length);
			arr[tokenIds0.length + tokenIds1.length + 2] = sepTokenId;
		}
		return arr;
	}

	@Override
	public int[] createTokenTypeIdsFromSequences(int[] tokenIds0, int[] tokenIds1) {
		return (null == tokenIds1)
			? TokenizerUtils.nCopiesArray(0, tokenIds0.length + 2)
			: TokenizerUtils.createArrayWithCopies(0, tokenIds0.length + 2, 1, tokenIds1.length + 1);
	}

	@Override
	protected int[] getSpecialTokensMask(int[] ids, int[] pairIds) {
		int[] arr;
		if (null == pairIds) {
			arr = new int[ids.length + 2];
			Arrays.fill(arr, 0);
			arr[0] = 1;
			arr[ids.length + 1] = 1;
		} else {
			arr = new int[ids.length + pairIds.length + 3];
			Arrays.fill(arr, 0);
			arr[0] = 1;
			arr[ids.length + 1] = 1;
			arr[ids.length + pairIds.length + 2] = 1;
		}
		return arr;
	}

	/**
	 * Constructs a {@link BasicTokenizer} that will run basic tokenization (punctuation splitting, lower casing,
	 * etc.).
	 */
	public static class BasicTokenizer {

		Set <String> neverSplit;
		boolean doLowerCase;
		boolean tokenizeChineseChars;
		Boolean stripAccents;

		/**
		 * Constructs a {@link BasicTokenizer} that will run basic tokenization (punctuation splitting, lower casing,
		 * etc.).
		 *
		 * @param doLowerCase          Whether or not to lowercase the input when tokenizing.
		 * @param neverSplit           Collection of tokens which will never be split during tokenization.
		 * @param tokenizeChineseChars Whether or not to tokenize Chinese characters. This should likely be deactivated
		 *                             for Japanese (see this `issue <https://github
		 *                             .com/huggingface/transformers/issues/328>`__).
		 * @param stripAccents         Whether or not to strip all accents. If this option is not specified, then it
		 *                             will be determined by the value for :obj:`lowercase` (as in the original BERT).
		 */
		public BasicTokenizer(boolean doLowerCase, Set <String> neverSplit, boolean tokenizeChineseChars,
							  Boolean stripAccents) {
			this.neverSplit = (null == neverSplit) ? Collections.emptySet() : neverSplit;
			this.doLowerCase = doLowerCase;
			this.tokenizeChineseChars = tokenizeChineseChars;
			this.stripAccents = stripAccents;
		}

		public BasicTokenizer() {
			this(true, null, true, null);
		}

		/**
		 * Basic Tokenization of a piece of text. Split on "white spaces" only, for sub-word tokenization, see
		 * WordPieceTokenizer.
		 *
		 * @param text       Text to be tokenized.
		 * @param neverSplit List of token not to split.
		 * @return
		 */
		public String[] tokenize(String text, Set <String> neverSplit) {
			if (null != neverSplit) {
				neverSplit = new HashSet <>(neverSplit);
			} else {
				neverSplit = new HashSet <>();
			}
			neverSplit.addAll(this.neverSplit);

			text = cleanText(text);
			if (this.tokenizeChineseChars) {
				text = tokenizeChineseChars(text);
			}

			String[] origTokens = whitespaceTokenize(text);
			List <String> splitTokens = new ArrayList <>();
			for (String token : origTokens) {
				if (!neverSplit.contains(token)) {
					if (doLowerCase) {
						token = token.toLowerCase(Locale.ROOT);
						if (null == stripAccents || stripAccents) {
							token = runStripAccents(token);
						}
					} else if (null != stripAccents && stripAccents) {
						token = runStripAccents(token);
					}
				}
				splitTokens.addAll(runSplitOnPunc(token, neverSplit));
			}
			return whitespaceTokenize(String.join(" ", splitTokens));
		}

		public String[] tokenize(String text) {
			return this.tokenize(text, null);
		}

		/**
		 * Strips accents from a piece of text.
		 *
		 * @param text
		 * @return
		 */
		static String runStripAccents(String text) {
			text = Normalizer.normalize(text, Form.NFD);
			int[] cps = text.codePoints()
				.filter(d -> Character.getType(d) != Character.NON_SPACING_MARK)
				.toArray();
			return new String(cps, 0, cps.length);
		}

		/**
		 * Splits punctuation on a piece of text.
		 *
		 * @param text
		 * @param neverSplit
		 * @return
		 */
		static List <String> runSplitOnPunc(String text, Set <String> neverSplit) {
			if (neverSplit.contains(text)) {
				return Collections.singletonList(text);
			}
			int[] cps = text.codePoints().toArray();
			boolean startNewWord = true;

			List <String> output = new ArrayList <>();

			for (int cp : cps) {
				if (isPunctuation(cp)) {
					output.add(new String(new int[] {cp}, 0, 1));
					startNewWord = true;
				} else {
					if (startNewWord) {
						output.add("");
					}
					startNewWord = false;
					output.set(
						output.size() - 1,
						output.get(output.size() - 1) + new String(new int[] {cp}, 0, 1)
					);
				}
			}
			return output;
		}

		/**
		 * Performs invalid character removal and whitespace cleanup on text.
		 *
		 * @param text Text to be cleaned.
		 * @return
		 */
		static String cleanText(String text) {
			int[] cps = text.codePoints().filter(
				d -> !(d == 0 || d == 0xFFFD || isControl(d))
			).map(
				d -> isWhitespace(d) ? ' ' : d
			).toArray();
			return new String(cps, 0, cps.length);
		}

		/**
		 * Adds whitespace around any CJK character.
		 *
		 * @param text Text to be tokenized.
		 * @return
		 */
		public static String tokenizeChineseChars(String text) {
			int[] cps = text.codePoints().flatMap(
				d -> (isChineseChar(d)
					? IntStream.of(' ', d, ' ')
					: IntStream.of(d))
			).toArray();
			return new String(cps, 0, cps.length);
		}

		/**
		 * Checks whether CP is the codepoint of a CJK character.
		 *
		 * @param cp codepoint
		 * @return
		 */
		public static boolean isChineseChar(int cp) {
			return (cp >= 0x4E00 && cp <= 0x9FFF)
				|| (cp >= 0x3400 && cp <= 0x4DBF)
				|| (cp >= 0x20000 && cp <= 0x2A6DF)
				|| (cp >= 0x2A700 && cp <= 0x2B73F)
				|| (cp >= 0x2B740 && cp <= 0x2B81F)
				|| (cp >= 0x2B820 && cp <= 0x2CEAF)
				|| (cp >= 0xF900 && cp <= 0xFAFF)
				|| (cp >= 0x2F800 && cp <= 0x2FA1F);
		}
	}

	/**
	 * Runs WordPiece tokenization.
	 */
	public static class WordpieceTokenizer {
		Map <String, Integer> vocab;
		String unkToken;
		int maxInputCharsPerWord;

		public WordpieceTokenizer(Map <String, Integer> vocab, String unkToken, int maxInputCharsPerWord) {
			this.vocab = vocab;
			this.unkToken = unkToken;
			this.maxInputCharsPerWord = maxInputCharsPerWord;
		}

		public WordpieceTokenizer(Map <String, Integer> vocab, String unkToken) {
			this(vocab, unkToken, 100);
		}

		/**
		 * Tokenizes a piece of text into its word pieces. This uses a greedy longest-match-first algorithm to perform
		 * tokenization using the given vocabulary.
		 * <p>
		 * For example, :obj:`input = "unaffable"` wil return as output :obj:`["un", "##aff", "##able"]`.
		 *
		 * @param text A single token or whitespace separated tokens. This should have already been passed through
		 *             `BasicTokenizer`.
		 * @return A list of wordpiece tokens.
		 */
		public String[] tokenizer(String text) {
			List<String> outputTokens = new ArrayList <>();
			for (String token : whitespaceTokenize(text)) {
				int[] cps = token.codePoints().toArray();
				if (cps.length > maxInputCharsPerWord) {
					outputTokens.add(unkToken);
					continue;
				}

				boolean isBad = false;
				int start = 0;
				List <String> subTokens = new ArrayList <>();
				while (start < cps.length) {
					int end = cps.length;
					String curSubstr = null;
					while (start < end) {
						String substr = new String(cps, start, end - start);
						if (start > 0) {
							substr = "##" + substr;
						}
						if (vocab.containsKey(substr)) {
							curSubstr = substr;
							break;
						}
						end -= 1;
					}
					if (null == curSubstr) {
						isBad = true;
						break;
					}
					subTokens.add(curSubstr);
					start = end;
				}

				if (isBad) {
					outputTokens.add(unkToken);
				} else {
					outputTokens.addAll(subTokens);
				}
			}
			return outputTokens.toArray(new String[0]);
		}
	}
}
