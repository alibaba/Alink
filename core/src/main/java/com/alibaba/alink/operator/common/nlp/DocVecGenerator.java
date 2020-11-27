package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.params.nlp.HasPredMethod;

import java.util.HashMap;
import java.util.List;

public class DocVecGenerator {
	private HashMap <String, DenseVector> embed = new HashMap <>();

	private String wordDelimiter;
	private HasPredMethod.PredMethod inferMethod;

	public DocVecGenerator(
		List <Row> dict,
		String wordDelimiter,
		HasPredMethod.PredMethod inferMethod) {

		this.wordDelimiter = wordDelimiter;
		this.inferMethod = inferMethod;
		for (Row row : dict) {
			embed.put((String) row.getField(0),
				(DenseVector) VectorUtil.getVector(VectorUtil.getVector(row.getField(1))));
		}
	}

	public String transform(String content) {
		if (null == content) {
			return null;
		}

		String[] tokens = content.split(wordDelimiter);

		DenseVector dvec = null;
		double cnt = 0;
		for (String word : tokens) {
			DenseVector t = embed.get(word);
			if (null != t) {
				if (null != dvec) {
					dvec = inferMethod.getFunc().apply(dvec, t);
				} else {
					dvec = t.clone();
				}

				cnt += 1.0;
			}
		}

		if (null == dvec) {
			return null;
		} else {
			if (inferMethod == HasPredMethod.PredMethod.AVG) {
				dvec.scaleEqual(1.0 / cnt);
			}
			return VectorUtil.toString(dvec);
		}
	}

}
