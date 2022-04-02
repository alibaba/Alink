package com.alibaba.alink.operator.common.nlp.bert;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.Kwargs;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.PreTrainedTokenizer;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.SingleEncoding;
import com.alibaba.alink.params.tensorflow.bert.HasTextCol;
import com.alibaba.alink.params.tensorflow.bert.HasTextPairCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

import java.util.Arrays;

abstract class PreTrainedTokenizerMapper extends Mapper {

	// Don't use prefix starting with underscore, since TF doesn't recognize such names.
	private static final String SAFE_PREFIX = "alink_tokenizer_";

	final String textCol;
	final String textPairCol;
	final EncodingKeys[] outputKeys;

	protected PreTrainedTokenizer tokenizer;
	protected Kwargs encodeConfig = Kwargs.empty();


	public PreTrainedTokenizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		textCol = params.get(HasTextCol.TEXT_COL);
		textPairCol = params.contains(HasTextPairCol.TEXT_PAIR_COL)
			? params.get(HasTextPairCol.TEXT_PAIR_COL)
			: null;
		outputKeys = calcOutputKeys(params);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		String text = (String) selection.get(0);
		String textPair = null != textPairCol
			? (String) selection.get(1)
			: null;
		SingleEncoding batchEncoding = tokenizer.encodePlus(text, textPair, encodeConfig);
		for (int i = 0; i < outputKeys.length; i += 1) {
			int[] data = batchEncoding.get(outputKeys[i]);
			result.set(i, new IntTensor(data));
		}
	}

	protected String[] calcSelectedCols(Params params) {
		boolean isPair = params.contains(HasTextPairCol.TEXT_PAIR_COL);
		return isPair
			? new String[] {params.get(HasTextCol.TEXT_COL), params.get(HasTextPairCol.TEXT_PAIR_COL)}
			: new String[] {params.get(HasTextCol.TEXT_COL)};
	}

	public static String prependPrefix(String s) {
		return SAFE_PREFIX + s;
	}

	protected abstract EncodingKeys[] calcOutputKeys(Params params);

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] selectedCols = calcSelectedCols(params);
		String[] outputCols = Arrays.stream(calcOutputKeys(params))
			.map(d -> prependPrefix(d.label))
			.toArray(String[]::new);
		TypeInformation <?>[] outputTypes = new TypeInformation <?>[outputCols.length];
		Arrays.fill(outputTypes, AlinkTypes.INT_TENSOR);

		return Tuple4.of(
			selectedCols,
			outputCols, outputTypes,
			params.get(HasReservedColsDefaultAsNull.RESERVED_COLS)
		);
	}
}
