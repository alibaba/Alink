package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.feature.TargetEncoderPredictParams;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.feature.TargetEncoderConverter.SEPARATOR;

public class TargetEncoderModelMapper extends ModelMapper {

	private String[] selectedCols;
	private TargetEncoderModelData modelData;
	int resSize;


	public TargetEncoderModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < resSize; i++) {
			String key = selection.get(i).toString();
			result.set(i, modelData.getData(selectedCols[i]).get(key));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		if (params.contains(TargetEncoderPredictParams.SELECTED_COLS)) {
			selectedCols = params.get(TargetEncoderPredictParams.SELECTED_COLS);
		} else {
			selectedCols = modelSchema.getFieldNames()[1].split(SEPARATOR);
		}
		resSize = selectedCols.length;
		String[] resCols = params.get(TargetEncoderPredictParams.OUTPUT_COLS);
		if (resCols.length != resSize) {
			throw new RuntimeException("Output column size must be equal to input column size.");
		}
		TypeInformation[] resTypes = new TypeInformation[resSize];
		Arrays.fill(resTypes, Types.DOUBLE);
		return Tuple4.of(selectedCols, resCols, resTypes, params.get(TargetEncoderPredictParams.RESERVED_COLS));
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new TargetEncoderConverter().load(modelRows);
	}
}
