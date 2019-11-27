package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.SISOModelMapper;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.nlp.HasPredMethod;
import com.alibaba.alink.params.shared.delimiter.HasWordDelimiter;

import java.util.List;

public class Word2VecModelMapper extends SISOModelMapper {
	DocVecGenerator generator;

	public Word2VecModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return Types.STRING;
	}

	@Override
	protected Object predictResult(Object input) throws Exception {
		return generator.transform((String) input);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		Word2VecModelDataConverter word2VecModel = new Word2VecModelDataConverter();
		word2VecModel.load(modelRows);

		generator = new DocVecGenerator(
			word2VecModel.modelRows,
			params.get(HasWordDelimiter.WORD_DELIMITER),
			DocVecGenerator.InferVectorMethod.valueOf(params.get(HasPredMethod.PRED_METHOD).trim().toUpperCase()));
	}
}
