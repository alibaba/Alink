package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

/**
 * This mapper changes a row values to range [-1, 1] by dividing through the maximum absolute value of each feature.
 */
public class MaxAbsScalerModelMapper extends ModelMapper {
	private static final long serialVersionUID = -358829029280240904L;
	private double[] maxAbs;

	/**
	 * Constructor.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public MaxAbsScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Tuple4<String[], String[], TypeInformation<?>[], String[]> prepareIoSchema(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] selectedColNames = RichModelDataConverter.extractSelectedColNames(modelSchema);
		TypeInformation[] selectedColTypes = RichModelDataConverter.extractSelectedColTypes(modelSchema);

		String[] outputColNames = params.get(SrtPredictMapperParams.OUTPUT_COLS);
		if (outputColNames == null) {
			outputColNames = selectedColNames;
		}
		return Tuple4.of(selectedColNames, outputColNames, selectedColTypes, null);
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data.
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		MaxAbsScalerModelDataConverter converter = new MaxAbsScalerModelDataConverter();
		maxAbs = converter.load(modelRows);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (null == selection || selection.length() == 0) {
			return;
		}
		for (int i = 0; i < selection.length(); i++) {
			Object obj = selection.get(i);
			if (null != obj) {
				double d;
				if (obj instanceof Number) {
					d = ((Number) obj).doubleValue();
				} else {
					d = Double.parseDouble(obj.toString());
				}
				result.set(i, ScalerUtil.maxAbsScaler(this.maxAbs[i], d));
			}
		}
	}
}
