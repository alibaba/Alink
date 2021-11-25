package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.common.dl.BertResources;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.BertTokenizerImpl;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.BertTokenizerImpl.BasicTokenizer;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.BertTokenizerImpl.WordpieceTokenizer;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.Kwargs;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.SingleEncoding;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isControl;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isPunctuation;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.TokenizationUtils.isWhitespace;

public class BertTokenizerTest {

	@Test
	public void testChinese() {
		BasicTokenizer tokenizer = new BasicTokenizer();
		Assert.assertArrayEquals(
			new String[] {"ah", "\u535A", "\u63A8", "zz"},
			tokenizer.tokenize("ah\u535A\u63A8zz")
		);
	}

	@Test
	public void testBasicTokenizerLower() {
		BasicTokenizer tokenizer = new BasicTokenizer(true, null, true, null);
		Assert.assertArrayEquals(new String[] {"hello", "!", "how", "are", "you", "?"},
			tokenizer.tokenize(" \tHeLLo!how  \n Are yoU?  ")
		);
		Assert.assertArrayEquals(new String[] {"hello"},
			tokenizer.tokenize("H\u00E9llo"));
	}

	@Test
	public void testBasicTokenizerLowerStripAccentsFalse() {
		BasicTokenizer tokenizer = new BasicTokenizer(true, null, true, false);
		Assert.assertArrayEquals(
			new String[] {"hällo", "!", "how", "are", "you", "?"},
			tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ")
		);
		Assert.assertArrayEquals(
			new String[] {"h\u00E9llo"},
			tokenizer.tokenize("H\u00E9llo")
		);
	}

	@Test
	public void testBasicTokenizerLowerStripAccentsTrue() {
		BasicTokenizer tokenizer = new BasicTokenizer(true, null, true, true);
		Assert.assertArrayEquals(
			new String[] {"hallo", "!", "how", "are", "you", "?"},
			tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ")
		);
		Assert.assertArrayEquals(
			new String[] {"hello"},
			tokenizer.tokenize("H\u00E9llo")
		);
	}

	@Test
	public void testBasicTokenizerLowerStripAccentsDefault() {
		BasicTokenizer tokenizer = new BasicTokenizer(true, null, true, null);

		Assert.assertArrayEquals(
			new String[] {"hallo", "!", "how", "are", "you", "?"},
			tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ")
		);
		Assert.assertArrayEquals(
			new String[] {"hello"},
			tokenizer.tokenize("H\u00E9llo")
		);
	}

	@Test
	public void testBasicTokenizerNoLower() {
		BasicTokenizer tokenizer = new BasicTokenizer(false, null, true, null);
		Assert.assertArrayEquals(
			new String[] {"HeLLo", "!", "how", "Are", "yoU", "?"},
			tokenizer.tokenize(" \tHeLLo!how  \n Are yoU?  ")
		);
	}

	@Test
	public void testBasicTokenizerNoLowerStripAccentsFalse() {
		BasicTokenizer tokenizer = new BasicTokenizer(false, null, true, false);

		Assert.assertArrayEquals(
			new String[] {"HäLLo", "!", "how", "Are", "yoU", "?"},
			tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ")
		);
	}

	@Test
	public void testBasicTokenizerNoLowerStripAccentsTrue() {
		BasicTokenizer tokenizer = new BasicTokenizer(false, null, true, true);

		Assert.assertArrayEquals(
			new String[] {"HaLLo", "!", "how", "Are", "yoU", "?"},
			tokenizer.tokenize(" \tHäLLo!how  \n Are yoU?  ")
		);
	}

	@Test
	public void testBasicTokenizerRespectsNeverSplitTokens() {
		BasicTokenizer tokenizer = new BasicTokenizer(false, Collections.singleton("[UNK]"), true, null);

		Assert.assertArrayEquals(
			new String[] {"HeLLo", "!", "how", "Are", "yoU", "?", "[UNK]"},
			tokenizer.tokenize(" \tHeLLo!how  \n Are yoU? [UNK]")
		);
	}

	@Test
	public void testWordpieceTokenizer() {
		String[] vocabTokens = new String[] {"[UNK]", "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn",
			"##ing"};
		Map <String, Integer> vocab = new HashMap <>();
		for (int i = 0; i < vocabTokens.length; i += 1) {
			vocab.put(vocabTokens[i], i);
		}
		WordpieceTokenizer tokenizer = new WordpieceTokenizer(vocab, "[UNK]");

		Assert.assertArrayEquals(new String[0], tokenizer.tokenizer(""));
		Assert.assertArrayEquals(new String[] {"un", "##want", "##ed", "runn", "##ing"},
			tokenizer.tokenizer("unwanted running"));
		Assert.assertArrayEquals(new String[] {"[UNK]", "runn", "##ing"}, tokenizer.tokenizer("unwantedX running"));
	}

	@Test
	public void testIsWhitespace() {
		Assert.assertTrue(isWhitespace(' '));
		Assert.assertTrue(isWhitespace('\t'));
		Assert.assertTrue(isWhitespace('\r'));
		Assert.assertTrue(isWhitespace('\n'));
		Assert.assertTrue(isWhitespace('\u00A0'));

		Assert.assertFalse(isWhitespace('A'));
		Assert.assertFalse(isWhitespace('-'));
	}

	@Test
	public void testIsControl() {
		Assert.assertTrue(isControl('\u0005'));

		Assert.assertFalse(isControl('A'));
		Assert.assertFalse(isControl(' '));
		Assert.assertFalse(isControl('\t'));
		Assert.assertFalse(isControl('\r'));
	}

	@Test
	public void testIsPunctuation() {
		Assert.assertTrue(isPunctuation('-'));
		Assert.assertTrue(isPunctuation('$'));
		Assert.assertTrue(isPunctuation('`'));
		Assert.assertTrue(isPunctuation('.'));

		Assert.assertFalse(isPunctuation('A'));
		Assert.assertFalse(isPunctuation(' '));
	}

	@Test
	@Ignore
	public void testSequenceBuilders() {
		File workDir = PythonFileUtils.createTempDir(null);
		workDir.deleteOnExit();
		ArchivesUtils.downloadDecompressToDirectory(
			BertResources.getBertModelVocab("bert-base-uncased"),
			workDir
		);
		BertTokenizerImpl tokenizer = BertTokenizerImpl.fromPretrained(workDir.getAbsolutePath());

		Kwargs encodeConfig = Kwargs.of("add_special_tokens", false);

		int[] text = tokenizer.encode("sequence builders", encodeConfig);
		int[] text_2 = tokenizer.encode("multi-sequence build", encodeConfig);

		Assert.assertArrayEquals(new int[] {5537, 16472}, text);
		Assert.assertArrayEquals(new int[] {4800, 1011, 5537, 3857}, text_2);

		int[] encoded_sentence = tokenizer.buildInputsWithSpecialTokens(text);
		int[] encoded_pair = tokenizer.buildInputsWithSpecialTokens(text, text_2);

		int[] expected_encoded_sentence = new int[text.length + 2];
		expected_encoded_sentence[0] = 101;
		System.arraycopy(text, 0, expected_encoded_sentence, 1, text.length);
		expected_encoded_sentence[text.length + 1] = 102;

		int[] expected_encoded_pair = new int[text.length + text_2.length + 3];
		expected_encoded_pair[0] = 101;
		System.arraycopy(text, 0, expected_encoded_pair, 1, text.length);
		expected_encoded_pair[text.length + 1] = 102;
		System.arraycopy(text_2, 0, expected_encoded_pair, text.length + 2, text_2.length);
		expected_encoded_pair[text.length + text_2.length + 2] = 102;

		Assert.assertArrayEquals(expected_encoded_sentence, encoded_sentence);
		Assert.assertArrayEquals(expected_encoded_pair, encoded_pair);
	}

	@Test
	@Ignore
	public void testBatchSequenceBuilders() {
		File workDir = PythonFileUtils.createTempDir(null);
		workDir.deleteOnExit();
		ArchivesUtils.downloadDecompressToDirectory(
			BertResources.getBertModelVocab("bert-base-uncased"),
			workDir
		);
		BertTokenizerImpl tokenizer = BertTokenizerImpl.fromPretrained(workDir.getAbsolutePath());

		Kwargs encodeConfig = Kwargs.of("add_special_tokens", false);

		int[] text = tokenizer.encode("sequence builders", encodeConfig);
		int[] text_2 = tokenizer.encode("multi-sequence build", encodeConfig);

		Assert.assertArrayEquals(new int[] {5537, 16472}, text);
		Assert.assertArrayEquals(new int[] {4800, 1011, 5537, 3857}, text_2);

		int[] encoded_sentence = tokenizer.buildInputsWithSpecialTokens(text);
		int[] encoded_pair = tokenizer.buildInputsWithSpecialTokens(text, text_2);

		int[] expected_encoded_sentence = new int[text.length + 2];
		expected_encoded_sentence[0] = 101;
		System.arraycopy(text, 0, expected_encoded_sentence, 1, text.length);
		expected_encoded_sentence[text.length + 1] = 102;

		int[] expected_encoded_pair = new int[text.length + text_2.length + 3];
		expected_encoded_pair[0] = 101;
		System.arraycopy(text, 0, expected_encoded_pair, 1, text.length);
		expected_encoded_pair[text.length + 1] = 102;
		System.arraycopy(text_2, 0, expected_encoded_pair, text.length + 2, text_2.length);
		expected_encoded_pair[text.length + text_2.length + 2] = 102;

		Assert.assertArrayEquals(expected_encoded_sentence, encoded_sentence);
		Assert.assertArrayEquals(expected_encoded_pair, encoded_pair);
	}

	@Test
	@Ignore
	public void testEncodePlus() {
		File workDir = PythonFileUtils.createTempDir(null);
		workDir.deleteOnExit();
		ArchivesUtils.downloadDecompressToDirectory(
			BertResources.getBertModelVocab("bert-base-uncased"),
			workDir
		);
		BertTokenizerImpl tokenizer = BertTokenizerImpl.fromPretrained(workDir.getAbsolutePath());

		String text = "sequence builders";
		String text2 = "multi-sequence build";

		Kwargs kwargs = Kwargs.of("return_length", true);
		SingleEncoding encoding = tokenizer.encodePlus(text, text2, kwargs);
		System.out.println(encoding);
		Assert.assertArrayEquals(new int[] {101, 5537, 16472, 102, 4800, 1011, 5537, 3857, 102},
			encoding.get(EncodingKeys.INPUT_IDS_KEY));
		Assert.assertArrayEquals(new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1},
			encoding.get(EncodingKeys.TOKEN_TYPE_IDS_KEY));
		Assert.assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1},
			encoding.get(EncodingKeys.ATTENTION_MASK_KEY));
		Assert.assertArrayEquals(new int[] {9},
			encoding.get(EncodingKeys.LENGTH_KEY));
	}
}
