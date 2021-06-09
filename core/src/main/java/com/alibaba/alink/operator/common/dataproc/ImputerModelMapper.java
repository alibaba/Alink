package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

import static com.alibaba.alink.params.dataproc.HasStrategy.Strategy;

/**
 * This mapper fills missing values in a dataset with pre-defined strategy.
 */
public class ImputerModelMapper extends ModelMapper {
	private static final long serialVersionUID = 7755777228322816182L;
	private double[] values;
	private final Type[] type;
	private String fillValue;

	/**
	 * This is the Type enum, and for one Type take one action.
	 */
	private enum Type {
		DOUBLE,
		LONG,
		BIGINT,
		INT,
		INTEGER,
		FLOAT,
		SHORT,
		BYTE,
		BOOLEAN,
		STRING
	}

	/**
	 * Constructor.
	 *
	 * @param modelSchema the model schema.
	 * @param dataSchema  the data schema.
	 * @param params      the params.
	 */
	public ImputerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		TypeInformation[] selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);

		int length = selectedColTypes.length;
		this.type = new Type[length];
		for (int i = 0; i < length; i++) {
			this.type[i] = Type.valueOf(selectedColTypes[i].getTypeClass().getSimpleName().toUpperCase());
		}
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
		ImputerModelDataConverter converter = new ImputerModelDataConverter();
		Tuple3 <Strategy, double[], String> tuple2 = converter.load(modelRows);
		values = tuple2.f1;
		if (Strategy.VALUE.equals(tuple2.f0)) {
			if (tuple2.f2 == null) {
				throw new RuntimeException("In VALUE strategy, the filling value is necessary.");
			}
			fillValue = tuple2.f2.toLowerCase();
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (null == selection || selection.length() == 0) {
			return;
		}
		int n = selection.length();
		for (int idx = 0; idx < n; idx++) {
			if (selection.get(idx) == null) {
				switch (this.type[idx]) {
					case DOUBLE:
						result.set(idx, this.values == null ? Double.parseDouble(fillValue) : values[idx]);
						break;
					case LONG:
					case BIGINT:
						result.set(idx, this.values == null ? Long.parseLong(fillValue) : (long) values[idx]);
						break;
					case INT:
					case INTEGER:
						result.set(idx, this.values == null ? Integer.parseInt(fillValue) : (int) values[idx]);
						break;
					case FLOAT:
						result.set(idx, this.values == null ? Float.parseFloat(fillValue) : (float) values[idx]);
						break;
					case SHORT:
						result.set(idx, this.values == null ? Short.parseShort(fillValue) : (short) values[idx]);
						break;
					case BYTE:
						result.set(idx, this.values == null ? Byte.parseByte(fillValue) : (byte) values[idx]);
						break;
					case BOOLEAN:
						switch (fillValue) {
							case "true":
							case "1":
								result.set(idx, true);
								break;
							case "false":
							case "0":
								result.set(idx, false);
								break;
							default:
								throw new IllegalArgumentException("Missing value filling policy not correct!");
						}
						break;
					case STRING:
						if ("str_type_empty".equals(fillValue)) {
							result.set(idx, "");
						} else {
							result.set(idx, fillValue);
						}
						break;
					default:
						throw new NoSuchMethodException("Unsupported type!");
				}
			} else {
				result.set(idx, selection.get(idx));
			}
		}
	}
}
