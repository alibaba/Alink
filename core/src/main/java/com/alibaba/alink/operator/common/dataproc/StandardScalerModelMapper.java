package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

public class StandardScalerModelMapper extends ModelMapper {
	private static final long serialVersionUID = -4432074756726879752L;
	private double[] means;
	private double[] stddevs;

	public StandardScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Tuple4<String[], String[], TypeInformation<?>[], String[]> prepareIoSchema(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] selectedColNames = ImputerModelDataConverter.extractSelectedColNames(modelSchema);
		TypeInformation<?>[] selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);

		String[] outputColNames = params.get(SrtPredictMapperParams.OUTPUT_COLS);
		if (outputColNames == null || outputColNames.length == 0) {
			outputColNames = selectedColNames;
		}
		return Tuple4.of(selectedColNames, outputColNames, selectedColTypes, null);
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	@Override
	public void loadModel(List <Row> modelRows) {
		StandardScalerModelDataConverter converter = new StandardScalerModelDataConverter();
		Tuple4 <Boolean, Boolean, double[], double[]> tuple4 = converter.load(modelRows);

		means = tuple4.f2;
		stddevs = tuple4.f3;
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < result.length(); i++) {
			Object obj = selection.get(i);
			if (null != obj) {
				if (this.stddevs[i] > 0) {
					double d = (((Number) obj).doubleValue() - this.means[i]) / this.stddevs[i];
					result.set(i, d);
				} else {
					result.set(i, 0.0);
				}
			}
		}
	}
}
