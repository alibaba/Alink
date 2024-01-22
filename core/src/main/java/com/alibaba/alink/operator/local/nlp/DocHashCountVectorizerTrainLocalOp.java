package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
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
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelData;
import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelDataConverter;
import com.alibaba.alink.operator.common.nlp.NLPConstant;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Hash every word as a number, and save the Inverse document frequency(IDF) of every word in the document.
 * <p>
 * It's used together with DocHashCountVectorizerPredictLocalOp.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("文本哈希特征生成训练")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.nlp.DocHashCountVectorizer")
public class DocHashCountVectorizerTrainLocalOp extends LocalOperator <DocHashCountVectorizerTrainLocalOp>
	implements DocHashCountVectorizerTrainParams <DocHashCountVectorizerTrainLocalOp> {
	private static final HashFunction HASH = murmur3_32(0);

	public DocHashCountVectorizerTrainLocalOp() {
		super(new Params());
	}

	public DocHashCountVectorizerTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getSelectedCol());

		List <Row> data = in.getOutputTable().getRows();
		final int numFeatures = getNumFeatures();

		HashMap <Integer, Double> map = new HashMap <>(numFeatures);
		long cnt = 0;
		for (Row row : data) {
			cnt++;
			String content = (String) row.getField(index);
			String[] words = content.split(NLPConstant.WORD_DELIMITER);

			for (String word : words) {
				int hashValue = Math.abs(HASH.hashUnencodedChars(word).asInt());
				map.merge(Math.floorMod(hashValue, numFeatures), 1.0, Double::sum);
			}
		}

		double minDocFrequency = getMinDF();
		String featureType = getFeatureType().name();
		double minTF = getMinTF();

		minDocFrequency = minDocFrequency >= 1.0 ? minDocFrequency : minDocFrequency * cnt;
		Iterator <Map.Entry <Integer, Double>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry <Integer, Double> entry = it.next();
			if (entry.getValue() >= minDocFrequency) {
				entry.setValue(Math.log((cnt + 1.0) / (entry.getValue() + 1.0)));
			} else {
				it.remove();
			}
		}
		DocHashCountVectorizerModelData model = new DocHashCountVectorizerModelData();
		model.numFeatures = numFeatures;
		model.minTF = minTF;
		model.featureType = featureType;
		model.idfMap = map;

		RowCollector rowCollector = new RowCollector();
		new DocHashCountVectorizerModelDataConverter().save(model, rowCollector);
		TableSchema schema = new DocHashCountVectorizerModelDataConverter().getModelSchema();

		this.setOutputTable(new MTable(rowCollector.getRows(), schema));
	}

}
