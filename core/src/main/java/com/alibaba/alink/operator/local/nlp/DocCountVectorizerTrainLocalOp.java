package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelData;
import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelDataConverter;
import com.alibaba.alink.operator.common.nlp.NLPConstant;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Extract all words from the dataset.Record the document frequency(DF), word count(WC) and Inverse document
 * frequency(IDF) of every word as a model.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本特征生成训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.nlp.DocCountVectorizer")
public final class DocCountVectorizerTrainLocalOp extends LocalOperator <DocCountVectorizerTrainLocalOp>
	implements DocCountVectorizerTrainParams <DocCountVectorizerTrainLocalOp> {
	private static final String WORD_COL_NAME = "word";
	private static final String DOC_WORD_COUNT_COL_NAME = "doc_word_cnt";
	private static final String DOC_COUNT_COL_NAME = "doc_cnt";

	public DocCountVectorizerTrainLocalOp() {
		this(null);
	}

	public DocCountVectorizerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		DocCountVectorizerModelData resDocCountModel = generateDocCountModel(getParams(), in);

		RowCollector rowCollector = new RowCollector();
		new DocCountVectorizerModelDataConverter().save(resDocCountModel, rowCollector);
		TableSchema schema = new DocCountVectorizerModelDataConverter().getModelSchema();

		this.setOutputTable(new MTable(rowCollector.getRows(), schema));
	}

	public static DocCountVectorizerModelData generateDocCountModel(Params params, LocalOperator in) {
		String docColName = params.get(SELECTED_COL);
		String wordDelimiter = NLPConstant.WORD_DELIMITER;

		int index = TableUtil.findColIndexWithAssert(in.getSchema(), docColName);
		HashMap <String, Double> mapWordValue = new HashMap <>();
		HashMap <String, Double> mapDocCount = new HashMap <>();
		HashSet <String> buffer = new HashSet <>();
		for (Row row : in.getOutputTable().getRows()) {
			String content = row.getField(index).toString();
			if (null == content || content.length() == 0) {
				continue;
			}
			buffer.clear();
			for (String word : content.split(wordDelimiter)) {
				if (word.length() > 0) {
					buffer.add(word);
					mapWordValue.merge(word, 1.0, Double::sum);
				}
			}
			for (String word : buffer) {
				mapDocCount.merge(word, 1.0, Double::sum);
			}
		}

		long docCnt = in.getOutputTable().getNumRow();
		double maxDF = params.get(MAX_DF);
		double minDF = params.get(MIN_DF);

		ArrayList <Row> list = new ArrayList <>();
		for (Map.Entry <String, Double> entry : mapWordValue.entrySet()) {
			double df = mapDocCount.get(entry.getKey());
			if (df >= minDF && df <= maxDF) {
				double idf = Math.log((1.0 + docCnt) / (1.0 + df));
				list.add(Row.of(entry.getKey(), entry.getValue(), idf));
			}
		}

		MTable mt = new MTable(list,
			WORD_COL_NAME + " string, " + DOC_WORD_COUNT_COL_NAME + " double, " + DOC_COUNT_COL_NAME + " double");

		mt.orderBy(new String[] {DOC_WORD_COUNT_COL_NAME}, new boolean[] {false});

		int vocabSize = params.get(VOCAB_SIZE);
		if (mt.getNumRow() > vocabSize) {
			mt = mt.subTable(0, vocabSize);
		}

		String featureType = params.get(DocHashCountVectorizerTrainParams.FEATURE_TYPE).name();
		double minTF = params.get(DocHashCountVectorizerTrainParams.MIN_TF);

		List <String> data = new ArrayList <>();
		Tuple3 <String, Double, Integer> feature = Tuple3.of(null, null, null);
		int cnt = 0;
		for (Row row : mt.getRows()) {
			feature.f0 = (String) row.getField(0);
			feature.f1 = (Double) row.getField(2);
			feature.f2 = cnt++;

			data.add(JsonConverter.toJson(feature));
		}

		DocCountVectorizerModelData modelData = new DocCountVectorizerModelData();
		modelData.featureType = featureType;
		modelData.minTF = minTF;
		modelData.list = data;

		return modelData;
	}

}
