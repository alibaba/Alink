package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.classification.KnnPredictParams;
import com.alibaba.alink.params.classification.KnnTrainParams;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KnnMapper extends ModelMapper {
	private static final long serialVersionUID = -6357517568280870848L;
	private NearestNeighborsMapper mapper;
	private OutputColsHelper outputColsHelper;
	private boolean isPredDetail;
	private int[] selectedIndices;
	private int selectIndex;
	private Type idType;

	public KnnMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		params.set(NearestNeighborPredictParams.SELECTED_COL, dataSchema.getFieldNames()[0]);
		params.set(NearestNeighborPredictParams.TOP_N, params.get(KnnPredictParams.K));

		this.mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);

		String[] keepColNames = this.params.get(KnnPredictParams.RESERVED_COLS);
		String predResultColName = this.params.get(KnnPredictParams.PREDICTION_COL);
		isPredDetail = params.contains(KnnPredictParams.PREDICTION_DETAIL_COL);
		if (isPredDetail) {
			String predDetailColName = params.get(KnnPredictParams.PREDICTION_DETAIL_COL);
			this.outputColsHelper = new OutputColsHelper(dataSchema,
				new String[] {predResultColName, predDetailColName},
				new TypeInformation[] {this.mapper.getIdType(), Types.STRING}, keepColNames);
		} else {
			this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, this.mapper.getIdType(),
				keepColNames);
		}
		this.idType = this.mapper.getIdType().getTypeClass();
	}

	public static Tuple2 <Object, String> getKnn(Tuple2 <List <Object>, List <Object>> tuple) {
		double percent = 1.0 / tuple.f0.size();
		Map <Object, Double> detail = new HashMap <>(0);

		for (Object obj : tuple.f0) {
			detail.merge(obj, percent, (a, b) -> (a + b));
		}

		double max = 0.0;
		Object prediction = null;

		for (Map.Entry <Object, Double> entry : detail.entrySet()) {
			if (entry.getValue() > max) {
				max = entry.getValue();
				prediction = entry.getKey();
			}
		}

		return Tuple2.of(prediction, JsonConverter.toJson(detail));
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.mapper.loadModel(modelRows);
		Params meta = this.mapper.getMeta();
		String[] featureCols = meta.get(KnnTrainParams.FEATURE_COLS);
		if (null != featureCols) {
			selectedIndices = TableUtil.findColIndicesWithAssertAndHint(getDataSchema(), featureCols);
		} else {
			selectIndex = TableUtil.findColIndexWithAssertAndHint(getDataSchema(), meta.get(KnnTrainParams
				.VECTOR_COL));
		}
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) {
		Vector vector;
		if (null != selectedIndices) {
			vector = new DenseVector(selectedIndices.length);
			for (int i = 0; i < selectedIndices.length; i++) {
				Preconditions.checkNotNull(row.getField(selectedIndices[i]), "There is NULL in featureCols!");
				vector.set(i, ((Number) row.getField(selectedIndices[i])).doubleValue());
			}
		} else {
			vector = VectorUtil.getVector(row.getField(selectIndex));
		}
		String s = (String) this.mapper.predictResult(vector.toString());
		Tuple2 <Object, String> tuple2 = getKnn(NearestNeighborsMapper.extractKObject(s, this.idType));

		if (isPredDetail) {
			return outputColsHelper.getResultRow(row, Row.of(tuple2.f0, tuple2.f1));
		} else {
			return outputColsHelper.getResultRow(row, Row.of(tuple2.f0));
		}
	}
}
