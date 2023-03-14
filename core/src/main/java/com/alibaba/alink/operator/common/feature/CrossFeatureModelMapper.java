package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.params.feature.CrossFeaturePredictParams;
import com.alibaba.alink.params.feature.CrossFeatureTrainParams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CrossFeatureModelMapper extends ModelMapper {

	int[] selectedColIndices;
	String[] dataColNames;

	HashMap <String, Integer>[] tokenAndIndex;
	int[] nullIndex;
	int[] carry;
	int[] dataIndices;
	int svLength = 0;

	public CrossFeatureModelMapper(TableSchema modelSchema, TableSchema dataSchema,
								   Params params) {
		super(modelSchema, dataSchema, params);
		dataColNames = dataSchema.getFieldNames();
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selectedColIndices.length; i++) {
			Object data = selection.get(selectedColIndices[i]);
			if (data == null) {
				if (nullIndex[i] != -1) {
					dataIndices[i] = nullIndex[i];
				} else {
					SparseVector sv = new SparseVector(svLength);
					result.set(0, sv);
					return;
				}
			} else if (tokenAndIndex[i].containsKey(data.toString())) {
				dataIndices[i] = tokenAndIndex[i].get(data.toString());
			} else {
				SparseVector sv = new SparseVector(svLength);
				result.set(0, sv);
				return;
			}
		}
		int resIndex = 0;
		for (int i = 0; i < carry.length; i++) {
			resIndex += carry[i] * dataIndices[i];
		}
		SparseVector sv = new SparseVector(svLength, new int[] {resIndex}, new double[] {1.0});
		result.set(0, sv);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] outColNames = new String[] {params.get(CrossFeaturePredictParams.OUTPUT_COL)};
		TypeInformation[] outTypes = new TypeInformation[] {AlinkTypes.SPARSE_VECTOR};
		return Tuple4.of(dataSchema.getFieldNames(), outColNames, outTypes, dataSchema.getFieldNames());
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		MultiStringIndexerModelData data = new MultiStringIndexerModelDataConverter()
			.load(modelRows);
		String[] selectedCols = data.meta.get(CrossFeatureTrainParams.SELECTED_COLS);
		selectedColIndices = TableUtil.findColIndices(dataColNames, selectedCols);
		int featureNumber = data.tokenNumber.size();
		tokenAndIndex = new HashMap[featureNumber];
		nullIndex = new int[featureNumber];
		Arrays.fill(nullIndex, -1);
		carry = new int[featureNumber];
		carry[0] = 1;
		for (int i = 0; i < featureNumber - 1; i++) {
			carry[i + 1] = (int) ((data.tokenNumber.get(i)) * carry[i]);
		}
		svLength = carry[featureNumber - 1] * (data.tokenNumber.get(featureNumber - 1).intValue());
		for (int i = 0; i < featureNumber; i++) {
			int thisSize = data.tokenNumber.get(i).intValue();
			tokenAndIndex[i] = new HashMap <>(thisSize);
		}

		for (Tuple3 <Integer, String, Long> tuple3 : data.tokenAndIndex) {
			if (tuple3.f1 == null) {
				nullIndex[tuple3.f0] = tuple3.f2.intValue();
			} else {
				tokenAndIndex[tuple3.f0].put(tuple3.f1, tuple3.f2.intValue());
			}
		}

		dataIndices = new int[featureNumber];
	}
}
