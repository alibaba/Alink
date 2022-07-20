package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClause;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseOperator;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil;
import com.alibaba.alink.operator.common.nlp.Word2VecModelDataConverter;
import com.alibaba.alink.params.dataproc.AggLookupParams;

import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

public class AggLookupModelMapper extends ModelMapper {
	private FeatureClauseOperator[] operators;
	private int[] sequenceLens;
	private int numAgg;

	private HashMap <String, DenseVector> embed = new HashMap <>();

	private String delimiter;
	private int vecSize;

	public AggLookupModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < numAgg; ++i) {
			String content = (String) selection.get(i);
			FeatureClauseOperator op = operators[i];
			int sequenceLen = sequenceLens[i] == -1 ? 0 : sequenceLens[i];
			if (null == selection.get(i)) {
				if (op.equals(FeatureClauseOperator.CONCAT)) {
					result.set(i, new DenseVector(sequenceLen));
				} else {
					result.set(i, null);
				}
				continue;
			}

			String[] tokens = content.split(delimiter);
			DenseVector dvec = null;
			double cnt = 0;
			if (op.equals(FeatureClauseOperator.CONCAT)) {
				dvec = new DenseVector(vecSize * (sequenceLen == 0 ? tokens.length : sequenceLen));
				int size = sequenceLen == 0 ? tokens.length : Math.min(sequenceLen, tokens.length);
				for (int j = 0; j < size; ++j) {
					DenseVector tmp = embed.get(tokens[j]);
					if (tmp != null) {
						for (int k = 0; k < vecSize; ++k) {
							dvec.set(j * vecSize + k, tmp.get(k));
						}
					}
				}
			} else {
				for (String word : tokens) {
					DenseVector t = embed.get(word);
					if (null != t) {
						if (null != dvec) {
							switch (op) {
								case MAX:
									MatVecOp.apply(dvec, t, dvec, new BiFunction <Double, Double, Double>() {
										@Override
										public Double apply(Double x, Double y) {
											return Math.max(x, y);
										}
									});
									break;
								case MIN:
									MatVecOp.apply(dvec, t, dvec, new BiFunction <Double, Double, Double>() {
										@Override
										public Double apply(Double x, Double y) {
											return Math.min(x, y);
										}
									});
									break;
								case AVG:
								case SUM:
									dvec.plusScaleEqual(t, 1.0);
									break;
								default:
									throw new AkUnsupportedOperationException("not support yet.");
							}

						} else {
							dvec = t.clone();
						}
						cnt += 1.0;
					}
				}
				if (op.equals(FeatureClauseOperator.AVG) && dvec != null) {
					dvec.scaleEqual(1.0 / cnt);
				}
			}
			result.set(i, dvec);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		FeatureClause[] featureClauses = FeatureClauseUtil.extractFeatureClauses(params.get(AggLookupParams.CLAUSE));
		this.numAgg = featureClauses.length;
		this.sequenceLens = new int[numAgg];
		this.operators = new FeatureClauseOperator[numAgg];
		String[] selectedCols = new String[numAgg];
		String[] outputCols = new String[numAgg];
		String[] reservedCols = params.get(AggLookupParams.RESERVED_COLS);
		if (reservedCols == null) {
			reservedCols = dataSchema.getFieldNames();
		}
		TypeInformation <?>[] outputTypes = new TypeInformation <?>[numAgg];
		for (int i = 0; i < numAgg; ++i) {
			this.operators[i] = featureClauses[i].op;
			selectedCols[i] = featureClauses[i].inColName;
			outputCols[i] = featureClauses[i].outColName;
			outputTypes[i] = AlinkTypes.DENSE_VECTOR;
			sequenceLens[i] = (featureClauses[i].inputParams.length == 1)
				? Integer.parseInt(featureClauses[i].inputParams[0].toString()) : -1;
		}
		return Tuple4.of(selectedCols, outputCols, outputTypes, reservedCols);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		Word2VecModelDataConverter word2VecModel = new Word2VecModelDataConverter();
		word2VecModel.load(modelRows);

		this.delimiter = params.get(AggLookupParams.DELIMITER);
		this.vecSize = VectorUtil.getVector(VectorUtil.getVector(word2VecModel.modelRows.get(0).getField(1))).size();
		for (Row row : word2VecModel.modelRows) {
			embed.put(row.getField(0).toString(),
				(DenseVector) VectorUtil.getVector(VectorUtil.getVector(row.getField(1))));
		}

	}

	@Override
	public ModelMapper createNew(List <Row> modelRows) {
		AggLookupModelMapper mapper =
			new AggLookupModelMapper(this.getModelSchema(), this.getDataSchema(), this.params);
		mapper.embed = this.embed;
		mapper.loadModel(modelRows);
		return mapper;
	}
}
