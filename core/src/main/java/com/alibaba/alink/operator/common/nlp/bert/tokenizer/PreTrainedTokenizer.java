package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.ATTENTION_MASK_KEY;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.INPUT_IDS_KEY;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.LENGTH_KEY;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.NUM_TRUNCATED_TOKENS;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.OVERFLOWING_TOKENS_KEY;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.SPECIAL_TOKENS_MASK_KEY;
import static com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys.TOKEN_TYPE_IDS_KEY;

public abstract class PreTrainedTokenizer implements Serializable {

	static String SPECIAL_TOKENS_MAP_FILE = "special_tokens_map.json";
	static String ADDED_TOKENS_FILE = "added_tokens.json";
	static String TOKENIZER_CONFIG_FILE = "tokenizer_config.json";

	enum SPECIAL_TOKENS {
		BOS_TOKEN,
		EOS_TOKEN,
		UNK_TOKEN,
		SEP_TOKEN,
		PAD_TOKEN,
		CLS_TOKEN,
		MASK_TOKEN
	}

	protected static int padTokenTypeId = 0;

	public enum PaddingStrategy {
		LONGEST,
		MAX_LENGTH,
		DO_NOT_PAD
	}

	public enum TruncationStrategy {
		ONLY_FIRST,
		ONLY_SECOND,
		LONGEST_FIRST,
		DO_NOT_TRUNCATE
	}

	Map <SPECIAL_TOKENS, String> specialTokenValues = new HashMap <>();

	static Map <String, String> additionalFilesNames = new HashMap <>();

	static {
		additionalFilesNames.put("added_tokens_file", ADDED_TOKENS_FILE);
		additionalFilesNames.put("special_tokens_map_file", SPECIAL_TOKENS_MAP_FILE);
		additionalFilesNames.put("tokenizer_config_file", TOKENIZER_CONFIG_FILE);
	}

	protected Kwargs config = Kwargs.empty();
	protected Map <String, Integer> addedTokensEncoder = new HashMap <>();
	protected Map <Integer, String> addedTokensDecoder = new HashMap <>();
	protected List <String> uniqueNoSplitTokens = new ArrayList <>();

	protected List <EncodingKeys> modelInputNames = Arrays.asList(INPUT_IDS_KEY,
		TOKEN_TYPE_IDS_KEY, ATTENTION_MASK_KEY);
	protected String paddingSide = "right";

	public static <T extends PreTrainedTokenizer> T fromPretrained(Class <T> cls, Kwargs kwargs) {
		String pretrained_model_name_or_path = kwargs.removeWithType("pretrained_model_name_or_path");
		File tokenizerConfigFileFile = Paths.get(pretrained_model_name_or_path, TOKENIZER_CONFIG_FILE).toFile();
		String content = TokenizerUtils.readFileToString(tokenizerConfigFileFile);

		Kwargs initKwargs = JsonConverter.fromJson(content, Kwargs.class);
		initKwargs.putAll(kwargs);

		// TODO: cls.max_model_input_sizes

		initKwargs.put(
			"vocab_file", Paths.get(pretrained_model_name_or_path, "vocab.txt").toAbsolutePath().toString(),
			"special_tokens_map_file", Paths.get(pretrained_model_name_or_path, "special_tokens_map.json")
				.toAbsolutePath().toString(),
			"name_or_path", pretrained_model_name_or_path);

		T tokenizer;
		try {
			Constructor <T> constructor = cls.getConstructor(Kwargs.class);
			tokenizer = constructor.newInstance(initKwargs);
		} catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
			throw new AkUnclassifiedErrorException(String.format("Cannot initialize class %s", cls.getCanonicalName()));
		}

		File specialTokensMapFile = Paths.get(pretrained_model_name_or_path, SPECIAL_TOKENS_MAP_FILE).toFile();
		if (specialTokensMapFile.exists()) {
			content = TokenizerUtils.readFileToString(specialTokensMapFile);
			Map <String, String> specialTokensMap = JsonConverter.fromJson(content,
				new TypeToken <Map <String, String>>() {}.getType());
			specialTokensMap.forEach((k, v) -> {
				tokenizer.specialTokenValues.put(
					SPECIAL_TOKENS.valueOf(k.toUpperCase()), v
				);
			});
		}

		// TODO: added_tokens_file is not None
		// TODO: sanitize_special_tokens
		return tokenizer;
	}

	int sanitizeSpecialTokens() {
		return addTokens(specialTokenValues.values().toArray(new String[0]), true);
	}

	/**
	 * Add a list of new tokens to the tokenizer class. If the new tokens are not in the vocabulary, they are added to
	 * it with indices starting from length of the current vocabulary.
	 *
	 * @param newTokens     Token(s) to add in vocabulary. A token is only added if it's not already in the vocabulary
	 *                      (tested by checking if the tokenizer assign the index of the ``unk_token`` to them).
	 * @param specialTokens Whether or not the tokens should be added as special tokens.
	 * @return The number of tokens actually added to the vocabulary.
	 */
	protected int addTokens(String[] newTokens, boolean specialTokens) {
		boolean doLowerCase = BooleanUtils.isTrue((Boolean) config.get("do_lower_case"));
		String unkToken = specialTokenValues.get(SPECIAL_TOKENS.UNK_TOKEN);
		List <String> tokensToAdd = new ArrayList <>();
		for (String token : newTokens) {
			if (!specialTokens && doLowerCase) {
				token = token.toLowerCase();
			}
			if (!token.equals(unkToken) && convertTokenToId(token) != convertTokenToId(unkToken) && !tokensToAdd
				.contains(token)) {
				tokensToAdd.add(token);
			}
		}

		for (int i = 0; i < tokensToAdd.size(); i += 1) {
			addedTokensEncoder.put(tokensToAdd.get(i), fullVocabSize() + i);
			addedTokensDecoder.put(fullVocabSize() + i, tokensToAdd.get(i));
		}

		if (specialTokens) {
			uniqueNoSplitTokens.addAll(Arrays.asList(newTokens));
		} else {
			uniqueNoSplitTokens.addAll(tokensToAdd);
		}
		Collections.sort(uniqueNoSplitTokens);
		return tokensToAdd.size();
	}

	private int convertTokenToId(String token) {
		return convertTokensToIds(new String[] {token})[0];
	}

	private int[] convertTokensToIds(String[] tokens) {
		return Arrays.stream(tokens).mapToInt(
			this::_convert_token_to_id_with_added_voc
		).toArray();
	}

	private int _convert_token_to_id_with_added_voc(String d) {
		if (addedTokensEncoder.containsKey(d)) {
			return addedTokensEncoder.get(d);
		}
		return convertTokenToIdImpl(d);
	}

	protected abstract int convertTokenToIdImpl(String d);

	int fullVocabSize() {
		return vocabSize() + addedTokensEncoder.size();
	}

	abstract int vocabSize();

	/**
	 * Converts a string in a sequence of tokens (string), using the tokenizer. Split in words for word-based
	 * vocabulary
	 * or sub-words for sub-word-based vocabularies (BPE/SentencePieces/WordPieces).
	 * <p>
	 * Do NOT take care of added tokens.
	 *
	 * @param text
	 * @return
	 */
	protected abstract String[] tokenizeImpl(String text);

	/**
	 * Converts a string in a sequence of tokens, using the tokenizer.
	 * <p>
	 * Split in words for word-based vocabulary or sub-words for sub-word-based vocabularies
	 * (BPE/SentencePieces/WordPieces). Takes care of added tokens.
	 *
	 * @param text The sequence to be encoded.
	 * @return The list of tokens.
	 */
	public String[] tokenize(String text, Kwargs kwargs) {
		// TODO: prepare_for_tokenization
		if (BooleanUtils.isTrue(config.getWithType("do_lower_case"))) {
			// TODO: must do!!!
		}
		return splitOnTokens(uniqueNoSplitTokens, text);
	}

	protected String[] splitOnTokens(List <String> tokens, String text) {
		text = text.trim();
		if (StringUtils.isEmpty(text)) {
			return new String[0];
		}
		if (tokens.isEmpty()) {
			return tokenizeImpl(text);
		}
		List <String> textList = Collections.singletonList(text);
		for (String token : tokens) {
			textList = textList.stream().flatMap(
				d -> uniqueNoSplitTokens.contains(d)
					? Arrays.stream(new String[] {d})
					: Arrays.stream(splitOnToken(token, d))
			).collect(Collectors.toList());
		}
		return textList.stream().flatMap(
			d -> uniqueNoSplitTokens.contains(d)
				? Arrays.stream(new String[] {d})
				: Arrays.stream(tokenizeImpl(d))
		).toArray(String[]::new);
	}

	private String[] splitOnToken(String token, String text) {
		// TODO: all_special_tokens_extended
		List <String> result = new ArrayList <>();
		String[] splitText = text.split(Pattern.quote(token));
		for (int i = 0; i < splitText.length; i += 1) {
			String subText = splitText[i];
			if (i < splitText.length - 1) {
				subText = StringUtils.stripEnd(subText, null);
			}
			if (i > 0) {
				subText = StringUtils.stripStart(subText, null);
			}
			if (i == 0 && subText.length() == 0) {
				result.add(subText);
			} else if (i == splitText.length - 1) {
				if (subText.length() > 0) {
					result.add(subText);
				}
			} else {
				if (subText.length() > 0) {
					result.add(subText);
				}
				result.add(token);
			}
		}
		return result.toArray(new String[0]);
	}

	private int[] getInputIds(String text) {
		// TODO: support more input types
		String[] tokens = tokenize(text, Kwargs.empty());
		return convertTokensToIds(tokens);
	}

	public SingleEncoding encodePlus(String text, String textPair, Kwargs kwargs) {
		kwargs = kwargs.clone().putIfAbsent(
			"add_special_tokens", true,
			"padding_strategy", PaddingStrategy.DO_NOT_PAD,
			"truncation_strategy", TruncationStrategy.DO_NOT_TRUNCATE,
			"stride", 0,
			"return_overflowing_tokens", false,
			"return_special_tokens_mask", false,
			"return_offsets_mapping", false,
			"return_length", false);
		int[] first_ids = getInputIds(text);
		int[] second_ids = null != textPair ? getInputIds(textPair) : null;
		return prepareForModel(first_ids, second_ids, kwargs);
	}

	public BatchEncoding batchEncodePlus(String[] texts, String[] textPairs, Kwargs kwargs) {
		int[][] batchFirstIds = Arrays.stream(texts)
			.map(this::getInputIds)
			.toArray(int[][]::new);
		int[][] batchSecondIds = null != textPairs
			? Arrays.stream(textPairs).map(this::getInputIds).toArray(int[][]::new)
			: null;
		return batchPrepareForModel(batchFirstIds, batchSecondIds, kwargs);
	}

	/**
	 * Converts a string to a sequence of ids (integer), using the tokenizer and vocabulary.
	 * <p>
	 * Same as doing ``self.convert_tokens_to_ids(self.tokenize(text))``.
	 *
	 * @param text
	 * @param textPair
	 * @param kwargs
	 * @return
	 */
	public int[] encode(String text, String textPair, Kwargs kwargs) {
		kwargs.putIfAbsent("add_special_tokens", true,
			"padding_strategy", PaddingStrategy.DO_NOT_PAD,
			"truncation_strategy", TruncationStrategy.DO_NOT_TRUNCATE,
			"stride", 0);
		SingleEncoding encodedInputs = encodePlus(text, textPair, kwargs);
		return encodedInputs.get(INPUT_IDS_KEY);
	}

	public int[] encode(String text, Kwargs kwargs) {
		return encode(text, null, kwargs);
	}

	/**
	 * Prepares a sequence of input id, or a pair of sequences of inputs ids so that it can be used by the model. It
	 * adds special tokens, truncates sequences if overflowing while taking into account the special tokens and manages
	 * a moving window (with user defined stride) for overflowing tokens.
	 *
	 * @param ids     Tokenized input ids of the first sequence. Can be obtained from a string by chaining the
	 *                ``tokenize`` and ``convert_tokens_to_ids`` methods.
	 * @param pairIds Optional. Tokenized input ids of the second sequence. Can be obtained from a string by chaining
	 *                the ``tokenize`` and ``convert_tokens_to_ids`` methods.
	 * @param kwargs
	 * @return
	 */
	protected SingleEncoding prepareForModel(int[] ids, int[] pairIds, Kwargs kwargs) {
		kwargs = kwargs.clone().putIfAbsent(
			"add_special_tokens", true,
			"padding_strategy", PaddingStrategy.DO_NOT_PAD,
			"truncation_strategy", TruncationStrategy.DO_NOT_TRUNCATE,
			"stride", 0,
			"return_overflowing_tokens", false,
			"return_special_tokens_mask", false,
			"return_offsets_mapping", false,
			"return_length", false);

		Boolean add_special_tokens = kwargs.getWithType("add_special_tokens");
		PaddingStrategy padding = kwargs.getWithType("padding_strategy");
		TruncationStrategy truncation = kwargs.getWithType("truncation_strategy");
		Integer max_length = kwargs.getWithType("max_length");
		int stride = kwargs.getWithType("stride");
		Integer pad_to_multiple_of = kwargs.getWithType("pad_to_multiple_of");

		Boolean return_token_type_ids = kwargs.getWithType("return_token_type_ids");
		Boolean return_attention_mask = kwargs.getWithType("return_attention_mask");
		boolean return_overflowing_tokens = kwargs.getWithType("return_overflowing_tokens");
		boolean return_special_tokens_mask = kwargs.getWithType("return_special_tokens_mask");
		boolean return_length = kwargs.getWithType("return_length");

		boolean pair = null != pairIds;
		int lenIds = ids.length;
		int lenPairIds = pair ? pairIds.length : 0;

		if (BooleanUtils.isTrue(return_token_type_ids) && BooleanUtils.isFalse(add_special_tokens)) {
			throw new AkIllegalArgumentException(
				"Asking to return token_type_ids while setting add_special_tokens to False " +
					"results in an undefined behavior. Please set add_special_tokens to True or " +
					"set return_token_type_ids to None.");
		}
		if (null == return_token_type_ids) {
			return_token_type_ids = modelInputNames.contains(TOKEN_TYPE_IDS_KEY);
		}
		if (null == return_attention_mask) {
			return_attention_mask = modelInputNames.contains(ATTENTION_MASK_KEY);
		}

		SingleEncoding encodedInputs = new SingleEncoding();

		// Compute the total size of the returned encodings
		int totalLen = lenIds + lenPairIds + (add_special_tokens ? numSpecialTokensToAdd(pair) : 0);
		if (!truncation.equals(TruncationStrategy.DO_NOT_TRUNCATE) && max_length != null && totalLen > max_length) {
			Triple <int[], int[], int[]> triple =
				truncateSequences(ids, pairIds, totalLen - max_length, truncation, stride);
			ids = triple.getLeft();
			pairIds = triple.getMiddle();
			int[] overflowingTokens = triple.getRight();
			if (return_overflowing_tokens) {
				encodedInputs.put(OVERFLOWING_TOKENS_KEY, overflowingTokens);
				encodedInputs.put(NUM_TRUNCATED_TOKENS, new int[]{totalLen - max_length});
			}
		}

		int[] sequence;
		int[] token_type_ids;
		if (add_special_tokens) {
			sequence = buildInputsWithSpecialTokens(ids, pairIds);
			token_type_ids = createTokenTypeIdsFromSequences(ids, pairIds);
		} else {
			sequence = pair ? ArrayUtils.addAll(ids, pairIds) : ids;
			token_type_ids = TokenizerUtils.nCopiesArray(0, lenIds + lenPairIds);
		}

		encodedInputs.put(INPUT_IDS_KEY, sequence);
		if (return_token_type_ids) {
			encodedInputs.put(TOKEN_TYPE_IDS_KEY, token_type_ids);
		}
		if (return_special_tokens_mask) {
			int[] specialTokensMask = BooleanUtils.isTrue(add_special_tokens)
				? getSpecialTokensMask(ids, pairIds)
				: TokenizerUtils.nCopiesArray(0, sequence.length);
			encodedInputs.put(SPECIAL_TOKENS_MASK_KEY, specialTokensMask);
		}

		if (!PaddingStrategy.DO_NOT_PAD.equals(padding) || return_attention_mask) {
			encodedInputs = padSingle(encodedInputs,
				max_length, padding, pad_to_multiple_of, return_attention_mask);
		}

		if (return_length) {
			encodedInputs.put(LENGTH_KEY, new int[] {encodedInputs.get(INPUT_IDS_KEY).length});
		}

		return encodedInputs;
	}

	protected int[] getSpecialTokensMask(int[] ids, int[] pairIds) {
		// TODO: already_has_special_tokens
		return TokenizerUtils.nCopiesArray(0, ids.length + (pairIds != null ? pairIds.length : 0));
	}

	/**
	 * Prepares a sequence of input id, or a pair of sequences of inputs ids so that it can be used by the model. It
	 * adds special tokens, truncates sequences if overflowing while taking into account the special tokens and manages
	 * a moving window (with user defined stride) for overflowing tokens
	 *
	 * @param kwargs
	 * @return
	 */
	protected BatchEncoding batchPrepareForModel(int[][] batchFirstIds, int[][] batchSecondIds,
												 Kwargs kwargs) {
		kwargs.putIfAbsent(
			"add_special_tokens", true,
			"padding_strategy", PaddingStrategy.DO_NOT_PAD,
			"truncation_strategy", TruncationStrategy.DO_NOT_TRUNCATE,
			"stride", 0,
			"return_overflowing_tokens", false,
			"return_special_tokens_mask", false,
			"return_length", false
		);

		// padding afterward
		Kwargs innerConfig = kwargs.clone().put(
			"padding_strategy", PaddingStrategy.DO_NOT_PAD,
			"return_attention_mask", false
		);
		innerConfig.remove("pad_to_multiple_of");

		BatchEncoding batchOutputs = new BatchEncoding();
		for (int i = 0; i < batchFirstIds.length; i += 1) {
			int[] firstIds = batchFirstIds[i];
			int[] secondIds = null != batchSecondIds ? batchSecondIds[i] : null;
			SingleEncoding outputs = prepareForModel(firstIds, secondIds, innerConfig);

			for (Entry <EncodingKeys, int[]> entry : outputs.entrySet()) {
				EncodingKeys k = entry.getKey();
				int[] v = entry.getValue();
				if (!batchOutputs.containsKey(k)) {
					batchOutputs.put(k, new int[batchFirstIds.length][]);
				}
				batchOutputs.get(k)[i] = v;
			}
		}

		PaddingStrategy padding = kwargs.getWithType("padding_strategy");
		Integer max_length = kwargs.getWithType("max_length");
		Integer pad_to_multiple_of = kwargs.getWithType("pad_to_multiple_of");

		Boolean return_attention_mask = kwargs.getWithType("return_attention_mask");
		batchOutputs = pad(batchOutputs, max_length, padding, pad_to_multiple_of, return_attention_mask);
		return batchOutputs;
	}

	/**
	 * Pad a single encoded input or a batch of encoded inputs up to predefined length or to the max sequence length in
	 * the batch.
	 * <p>
	 * Padding side (left/right) padding token ids are defined at the tokenizer level (with ``self .padding_side``,
	 * ``self.pad_token_id`` and ``self.pad_token_type_id``)
	 * <p>
	 * .. note::
	 * <p>
	 * If the ``encoded_inputs`` passed are dictionary of numpy arrays, PyTorch tensors or TensorFlow tensors, the
	 * result will use the same type unless you provide a different tensor type with ``return_tensors``. In the case of
	 * PyTorch tensors, you will lose the specific device of your tensors however.
	 *
	 * @param encodedInputs
	 * @param maxLength
	 * @param paddingStrategy
	 * @param padToMultipleOf
	 * @param returnAttentionMask
	 * @return
	 */
	protected BatchEncoding pad(BatchEncoding encodedInputs, Integer maxLength, PaddingStrategy paddingStrategy,
								Integer padToMultipleOf, Boolean returnAttentionMask) {
		int[][] requiredInput = encodedInputs.get(modelInputNames.get(0));
		int batchSize = requiredInput.length;

		if (PaddingStrategy.LONGEST.equals(paddingStrategy)) {
			maxLength = Arrays.stream(requiredInput)
				.mapToInt(d -> d.length)
				.max().getAsInt();
			paddingStrategy = PaddingStrategy.MAX_LENGTH;
		}

		BatchEncoding outputs = new BatchEncoding();
		for (int i = 0; i < batchSize; i += 1) {
			SingleEncoding singleEncoding = new SingleEncoding();
			for (Entry <EncodingKeys, int[][]> entry : encodedInputs.entrySet()) {
				EncodingKeys k = entry.getKey();
				int[][] v = entry.getValue();
				singleEncoding.put(k, v[i]);
			}
			singleEncoding = padSingle(
				singleEncoding, maxLength, paddingStrategy, padToMultipleOf, returnAttentionMask
			);
			for (Entry <EncodingKeys, int[]> entry : singleEncoding.entrySet()) {
				EncodingKeys k = entry.getKey();
				int[] v = entry.getValue();
				if (!outputs.containsKey(k)) {
					int[][] arr = new int[batchSize][];
					outputs.put(k, arr);
				}
				int[][] arr = outputs.get(k);
				arr[i] = v;
			}
		}
		return outputs;
	}

	/**
	 * Pad encoded inputs (on left/right and up to predefined length or max length in the batch)
	 *
	 * @param encodedInputs
	 * @param maxLength
	 * @param paddingStrategy
	 * @param padToMultipleOf
	 * @param returnAttentionMask
	 * @return
	 */
	protected SingleEncoding padSingle(SingleEncoding encodedInputs, Integer maxLength, PaddingStrategy paddingStrategy,
									  Integer padToMultipleOf, Boolean returnAttentionMask) {
		if (null == returnAttentionMask) {
			returnAttentionMask = modelInputNames.contains(ATTENTION_MASK_KEY);
		}
		int[] requiredInput = encodedInputs.get(modelInputNames.get(0));

		if (PaddingStrategy.LONGEST.equals(paddingStrategy)) {
			maxLength = requiredInput.length;
		}

		if (null != maxLength && null != padToMultipleOf && (maxLength % padToMultipleOf != 0)) {
			maxLength = ((maxLength / padToMultipleOf) + 1) * padToMultipleOf;
		}

		boolean needsToBePadded = (PaddingStrategy.DO_NOT_PAD != paddingStrategy) && (requiredInput.length
			!= maxLength);
		if (!needsToBePadded) {
			encodedInputs.put(ATTENTION_MASK_KEY, TokenizerUtils.nCopiesArray(1, requiredInput.length));
			return encodedInputs;
		}

		int difference = maxLength - requiredInput.length;
		if ("right".equals(paddingSide)) {
			if (returnAttentionMask) {
				int[] arr = new int[maxLength];
				Arrays.fill(arr, 0, requiredInput.length, 1);
				Arrays.fill(arr, requiredInput.length, maxLength, 0);
				encodedInputs.put(ATTENTION_MASK_KEY, arr);
			}
			if (encodedInputs.containsKey(TOKEN_TYPE_IDS_KEY)) {
				int[] prevArr = encodedInputs.get(TOKEN_TYPE_IDS_KEY);
				encodedInputs.put(TOKEN_TYPE_IDS_KEY,
					ArrayUtils.addAll(prevArr, TokenizerUtils.nCopiesArray(padTokenTypeId, difference)));
			}
			if (encodedInputs.containsKey(SPECIAL_TOKENS_MASK_KEY)) {
				int[] prevArr = encodedInputs.get(SPECIAL_TOKENS_MASK_KEY);
				encodedInputs.put(SPECIAL_TOKENS_MASK_KEY, ArrayUtils.addAll(prevArr, TokenizerUtils.nCopiesArray(1,
					difference)));
			}
			encodedInputs.put(modelInputNames.get(0),
				ArrayUtils.addAll(requiredInput, TokenizerUtils.nCopiesArray(padTokenTypeId, difference)));
		} else if ("left".equals(paddingSide)) {
			if (returnAttentionMask) {
				int[] arr = new int[maxLength];
				Arrays.fill(arr, 0, difference, 0);
				Arrays.fill(arr, difference, maxLength, 1);
				encodedInputs.put(ATTENTION_MASK_KEY, arr);
			}
			if (encodedInputs.containsKey(TOKEN_TYPE_IDS_KEY)) {
				int[] prevArr = encodedInputs.get(TOKEN_TYPE_IDS_KEY);
				encodedInputs.put(TOKEN_TYPE_IDS_KEY,
					ArrayUtils.addAll(TokenizerUtils.nCopiesArray(padTokenTypeId, difference), prevArr));
			}
			if (encodedInputs.containsKey(SPECIAL_TOKENS_MASK_KEY)) {
				int[] prevArr = encodedInputs.get(SPECIAL_TOKENS_MASK_KEY);
				encodedInputs.put(SPECIAL_TOKENS_MASK_KEY, ArrayUtils.addAll(TokenizerUtils.nCopiesArray(1, difference),
					prevArr));
			}
			int padTokenId = convertTokenToIdImpl(specialTokenValues.get(SPECIAL_TOKENS.PAD_TOKEN));
			encodedInputs.put(modelInputNames.get(0),
				ArrayUtils.addAll(TokenizerUtils.nCopiesArray(padTokenId, difference), requiredInput));
		} else {
			throw new AkUnsupportedOperationException(String.format("Invalid padding strategy: %s", paddingStrategy));
		}
		return encodedInputs;
	}

	/**
	 * Truncates a sequence pair in-place following the strategy.
	 *
	 * @param ids
	 * @param pairIds
	 * @param numTokensToRemove
	 * @param truncationStrategy
	 * @param stride
	 */
	protected Triple <int[], int[], int[]> truncateSequences(int[] ids, int[] pairIds, int numTokensToRemove,
															 TruncationStrategy truncationStrategy, int stride) {
		if (numTokensToRemove <= 0) {
			return Triple.of(ids, pairIds, new int[0]);
		}

		int[] overflowingTokens = new int[0];
		if (truncationStrategy.equals(TruncationStrategy.LONGEST_FIRST)) {
			// TODO: optimize array copy
			for (int i = 0; i < numTokensToRemove; i += 1) {
				if (null == pairIds || ids.length > pairIds.length) {
					int windowLen = overflowingTokens.length == 0
						? Math.min(ids.length, stride + 1)
						: 1;
					overflowingTokens = ArrayUtils.addAll(overflowingTokens,
						Arrays.copyOfRange(ids, ids.length - windowLen, ids.length));
					ids = ArrayUtils.remove(ids, ids.length - 1);
				} else {
					int windowLen = overflowingTokens.length == 0
						? Math.min(pairIds.length, stride + 1)
						: 1;
					overflowingTokens = ArrayUtils.addAll(overflowingTokens,
						Arrays.copyOfRange(pairIds, pairIds.length - windowLen, pairIds.length));
					pairIds = ArrayUtils.remove(pairIds, pairIds.length - 1);
				}
			}
		} else if (truncationStrategy.equals(TruncationStrategy.ONLY_FIRST)) {
			if (ids.length > numTokensToRemove) {
				int windowLen = Math.min(ids.length, stride + numTokensToRemove);
				overflowingTokens = Arrays.copyOfRange(ids, ids.length - windowLen, ids.length);
				ids = Arrays.copyOfRange(ids, 0, ids.length - numTokensToRemove);
			} else {
				System.err.println(
					String.format("Cannot remove %d tokens from first sequence %s",
						numTokensToRemove, Arrays.toString(ids)));
			}
		} else if (truncationStrategy.equals(TruncationStrategy.ONLY_SECOND) && null != pairIds) {
			if (pairIds.length > numTokensToRemove) {
				int windowLen = Math.min(pairIds.length, stride + numTokensToRemove);
				overflowingTokens = Arrays.copyOfRange(pairIds, pairIds.length - windowLen, pairIds.length);
				pairIds = Arrays.copyOfRange(pairIds, 0, pairIds.length - numTokensToRemove);
			} else {
				System.err.println(
					String.format("Cannot remove %d tokens from second sequence %s",
						numTokensToRemove, Arrays.toString(pairIds)));
			}
		}
		return Triple.of(ids, pairIds, overflowingTokens);
	}

	/**
	 * Returns the number of added tokens when encoding a sequence with special tokens. .. note:: This encodes a dummy
	 * input and checks the number of added tokens, and is therefore not efficient. Do not put this inside your
	 * training
	 * loop.
	 *
	 * @param pair Whether the number of added tokens should be computed in the case of a sequence pair or a single
	 *             sequence.
	 * @return Number of special tokens added to sequences.
	 */
	protected int numSpecialTokensToAdd(boolean pair) {
		return buildInputsWithSpecialTokens(new int[0], pair ? new int[0] : null).length;
	}

	/**
	 * Build model inputs from a sequence or a pair of sequence for sequence classification tasks by concatenating and
	 * adding special tokens.
	 * <p>
	 * This implementation does not add special tokens and this method should be overridden in a subclass.
	 *
	 * @param tokenIds0 The first tokenized sequence.
	 * @param tokenIds1 Optional. The second tokenized sequence.
	 * @return The model input with special tokens.
	 */
	public int[] buildInputsWithSpecialTokens(int[] tokenIds0, int[] tokenIds1) {
		return null == tokenIds1 ? tokenIds0 : ArrayUtils.addAll(tokenIds0, tokenIds1);
	}

	/**
	 * Create the token type IDs corresponding to the sequences passed. `What are token type IDs?
	 * <../glossary.html#token-type-ids>`__
	 * <p>
	 * Should be overridden in a subclass if the model has a special way of building those.
	 *
	 * @param tokenIds0 The first tokenized sequence.
	 * @param tokenIds1 Optional. The second tokenized sequence.
	 * @return The token type ids.
	 */
	public int[] createTokenTypeIdsFromSequences(int[] tokenIds0, int[] tokenIds1) {
		int[] arr;
		if (null == tokenIds1) {
			arr = new int[tokenIds0.length];
			Arrays.fill(arr, 0);
		} else {
			arr = new int[tokenIds0.length + tokenIds1.length];
			Arrays.fill(arr, 0, tokenIds0.length, 0);
			Arrays.fill(arr, tokenIds0.length, tokenIds0.length + tokenIds1.length, 1);
		}
		return arr;
	}
}
