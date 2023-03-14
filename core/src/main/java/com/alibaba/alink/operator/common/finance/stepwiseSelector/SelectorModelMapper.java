package com.alibaba.alink.operator.common.finance.stepwiseSelector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.vector.VectorAssemblerMapper;
import com.alibaba.alink.operator.common.feature.SelectorModelData;
import com.alibaba.alink.operator.common.feature.SelectorModelDataConverter;
import com.alibaba.alink.params.finance.SelectorPredictParams;

import java.util.List;

public class SelectorModelMapper extends ModelMapper {
	private static final long serialVersionUID = -4884089344356950010L;

	private SelectorModelData smd;
	private int[] selectedIndices;
	private int selectedIdx;

	public SelectorModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.smd = new SelectorModelDataConverter().load(modelRows);
		if (smd.vectorColNames != null) {
			selectedIndices = TableUtil.findColIndicesWithAssert(this.getDataSchema().getFieldNames(),
				smd.vectorColNames);
		} else {
			String colName = smd.vectorColName;
			if (params.contains(SelectorPredictParams.SELECTED_COL)) {
				colName = params.get(SelectorPredictParams.SELECTED_COL);
			}
			selectedIdx = TableUtil.findColIndexWithAssert(this.getDataSchema().getFieldNames(), colName);
		}
	}

	/**
	 * Returns the tuple of selectedCols, resultCols, resultTypes, reservedCols.
	 */
	@Override

	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		return Tuple4.of(dataSchema.getFieldNames(),
			new String[] {params.get(SelectorPredictParams.PREDICTION_COL)},
			new TypeInformation<?>[] {AlinkTypes.VECTOR},
			params.get(SelectorPredictParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) {
		Vector vec = null;
		if (smd.vectorColNames != null) {
			Object[] values = new Object[smd.vectorColNames.length];
			for (int i = 0; i < smd.vectorColNames.length; i++) {
				values[i] = VectorUtil.getVector(selection.get(selectedIndices[i]));
			}
			vec = (Vector) VectorAssemblerMapper.assembler(values);
		} else {
			vec = VectorUtil.getVector(selection.get(selectedIdx)).slice(smd.selectedIndices);
		}
		result.set(0, vec);
	}
}
