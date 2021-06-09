package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

/**
 * This mapper changes a row values to a specific range [min, max], the rescaled value is
 * <blockquote>
 * Rescaled(value) = \frac{value - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 * </blockquote>
 *
 * For the case \(E_{max} == E_{min}\), \(Rescaled(value) = 0.5 * (max + min)\).
 */
public class MinMaxScalerModelMapper extends ModelMapper {
	private static final long serialVersionUID = 9164408346601562197L;
	private double[] eMaxs;
	private double[] eMins;
	private double max;
	private double min;

	/**
	 * Constructor
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public MinMaxScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected Tuple4<String[], String[], TypeInformation<?>[], String[]> prepareIoSchema(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		String[] selectedColNames = ImputerModelDataConverter.extractSelectedColNames(modelSchema);
		TypeInformation[] selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);

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
		MinMaxScalerModelDataConverter converter = new MinMaxScalerModelDataConverter();
		Tuple4 <Double, Double, double[], double[]> tuple4 = converter.load(modelRows);

		this.min = tuple4.f0;
		this.max = tuple4.f1;
		this.eMins = tuple4.f2;
		this.eMaxs = tuple4.f3;
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
				d = ScalerUtil.minMaxScaler(d, eMins[i], eMaxs[i], max, min);
				result.set(i, d);
			}
		}
	}
}
