package com.alibaba.alink.operator.common.nlp.bert;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.dl.BertResources;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.BertTokenizerImpl;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.EncodingKeys;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.Kwargs;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.PreTrainedTokenizer.PaddingStrategy;
import com.alibaba.alink.operator.common.nlp.bert.tokenizer.PreTrainedTokenizer.TruncationStrategy;
import com.alibaba.alink.params.tensorflow.bert.HasBertModelName;
import com.alibaba.alink.params.tensorflow.bert.HasDoLowerCaseDefaultAsNull;
import com.alibaba.alink.params.tensorflow.bert.HasMaxSeqLengthDefaultAsNull;

import java.io.File;

@Internal
public class BertTokenizerMapper extends PreTrainedTokenizerMapper {

	public BertTokenizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public void open() {
		String modelName = params.get(HasBertModelName.BERT_MODEL_NAME);
		String vocabPath = BertResources.getBertModelVocab(modelName);

		File localModelDir;
		if (vocabPath.startsWith("file://")) {	// from plugin
			localModelDir = new File(vocabPath.substring("file://".length()));
		} else {
			localModelDir = PythonFileUtils.createTempDir(null);
			ArchivesUtils.downloadDecompressToDirectory(vocabPath, localModelDir);
			localModelDir.deleteOnExit();
		}

		Kwargs kwargs = Kwargs.empty();
		if (null != params.get(HasDoLowerCaseDefaultAsNull.DO_LOWER_CASE)) {
			kwargs.put("do_lower_case", params.get(HasDoLowerCaseDefaultAsNull.DO_LOWER_CASE));
		}
		tokenizer = BertTokenizerImpl.fromPretrained(localModelDir.getAbsolutePath(), kwargs);

		encodeConfig.put(
			"return_length", true,
			"truncation_strategy", TruncationStrategy.LONGEST_FIRST);
		if (params.contains(HasMaxSeqLengthDefaultAsNull.MAX_SEQ_LENGTH) &&
			null != params.get(HasMaxSeqLengthDefaultAsNull.MAX_SEQ_LENGTH)) {
			encodeConfig.put("padding_strategy", PaddingStrategy.MAX_LENGTH,
				"max_length", params.get(HasMaxSeqLengthDefaultAsNull.MAX_SEQ_LENGTH));
		}
	}

	@Override
	protected EncodingKeys[] calcOutputKeys(Params params) {
		return new EncodingKeys[] {
			EncodingKeys.INPUT_IDS_KEY,
			EncodingKeys.TOKEN_TYPE_IDS_KEY,
			EncodingKeys.ATTENTION_MASK_KEY,
			EncodingKeys.LENGTH_KEY
		};
	}
}
