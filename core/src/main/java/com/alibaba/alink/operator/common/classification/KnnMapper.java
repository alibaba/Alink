package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
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
	private final boolean isPredDetail;
	private int[] selectedIndices;
	private int selectIndex;
	private final Type idType;

	public KnnMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		params.set(NearestNeighborPredictParams.SELECTED_COL, dataSchema.getFieldNames()[0]);
		params.set(NearestNeighborPredictParams.TOP_N, params.get(KnnPredictParams.K));

		this.mapper = new NearestNeighborsMapper(modelSchema, dataSchema, params);
		isPredDetail = params.contains(KnnPredictParams.PREDICTION_DETAIL_COL);
		this.idType = this.mapper.getIdType().getTypeClass();
	}

	private Tuple2 <Object, String> getKnn(Tuple2 <List <Object>, List <Object>> tuple) {
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
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Vector vector;
		if (null != selectedIndices) {
			vector = new DenseVector(selectedIndices.length);
			for (int i = 0; i < selectedIndices.length; i++) {
				AkPreconditions.checkNotNull(selection.get(selectedIndices[i]), "There is NULL in featureCols!");
				vector.set(i, ((Number) selection.get(selectedIndices[i])).doubleValue());
			}
		} else {
			vector = VectorUtil.getVector(selection.get(selectIndex));
		}
		String s = (String) this.mapper.predictResult(vector);
		Tuple2 <Object, String> tuple2 = getKnn(NearestNeighborsMapper.extractKObject(s, this.idType));

		result.set(0, tuple2.f0);
		if (isPredDetail) {
			result.set(1, tuple2.f1);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {
		boolean isPredDetail = params.contains(KnnPredictParams.PREDICTION_DETAIL_COL);
		TypeInformation <?> idType = modelSchema.getFieldTypes()[modelSchema.getFieldNames().length - 1];
		String[] resultCols = isPredDetail ? new String[] {params.get(KnnPredictParams.PREDICTION_COL),
			params.get(KnnPredictParams.PREDICTION_DETAIL_COL)} : new String[] {params.get(
			KnnPredictParams.PREDICTION_COL)};
		TypeInformation[] resultTypes = isPredDetail ? new TypeInformation[] {idType, Types.STRING}
			: new TypeInformation[] {idType};

		return Tuple4.of(dataSchema.getFieldNames(), resultCols, resultTypes,
			params.get(KnnPredictParams.RESERVED_COLS));
	}
}
