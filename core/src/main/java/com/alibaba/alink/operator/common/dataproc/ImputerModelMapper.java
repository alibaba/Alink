package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.SrtPredictMapperParams;

import java.util.List;

import static com.alibaba.alink.params.dataproc.HasStrategy.Strategy;

/**
 * This mapper fills missing values in a dataset with pre-defined strategy.
 */
public class ImputerModelMapper extends ModelMapper {
	private static final long serialVersionUID = 7755777228322816182L;
	private int[] selectedColIndices;
	private double[] values;
	private Type[] type;
	private String fillValue;
	private OutputColsHelper predictResultColsHelper;

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
		String[] selectedColNames = ImputerModelDataConverter.extractSelectedColNames(modelSchema);
		TypeInformation[] selectedColTypes = ImputerModelDataConverter.extractSelectedColTypes(modelSchema);
		this.selectedColIndices = TableUtil.findColIndicesWithAssert(dataSchema, selectedColNames);

		String[] outputColNames = params.get(SrtPredictMapperParams.OUTPUT_COLS);
		if (outputColNames == null) {
			outputColNames = selectedColNames;
		}

		this.predictResultColsHelper = new OutputColsHelper(dataSchema, outputColNames, selectedColTypes, null);
		int length = selectedColTypes.length;
		this.type = new Type[length];
		for (int i = 0; i < length; i++) {
			this.type[i] = Type.valueOf(selectedColTypes[i].getTypeClass().getSimpleName().toUpperCase());
		}
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

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data.
	 */
	@Override
	public TableSchema getOutputSchema() {
		return this.predictResultColsHelper.getResultSchema();
	}

	/**
	 * Map operation method.
	 *
	 * @param row the input Row type data.
	 * @return one Row type data.
	 * @throws Exception This method may throw exceptions. Throwing
	 * an exception will cause the operation to fail.
	 */
	@Override
	public Row map(Row row) throws Exception {
		if (null == row) {
			return null;
		}
		int n = selectedColIndices.length;
		Row r = new Row(n);
		for (int idx = 0; idx < n; idx++) {
			if (row.getField(selectedColIndices[idx]) == null) {
				switch (this.type[idx]) {
					case DOUBLE:
						r.setField(idx, this.values == null ? Double.parseDouble(fillValue) : values[idx]);
						break;
					case LONG:
					case BIGINT:
						r.setField(idx, this.values == null ? Long.parseLong(fillValue) : (long) values[idx]);
						break;
					case INT:
					case INTEGER:
						r.setField(idx, this.values == null ? Integer.parseInt(fillValue) : (int) values[idx]);
						break;
					case FLOAT:
						r.setField(idx, this.values == null ? Float.parseFloat(fillValue) : (float) values[idx]);
						break;
					case SHORT:
						r.setField(idx, this.values == null ? Short.parseShort(fillValue) : (short) values[idx]);
						break;
					case BYTE:
						r.setField(idx, this.values == null ? Byte.parseByte(fillValue) : (byte) values[idx]);
						break;
					case BOOLEAN:
						switch (fillValue) {
							case "true":
							case "1":
								r.setField(idx, true);
								break;
							case "false":
							case "0":
								r.setField(idx, false);
								break;
							default:
								throw new IllegalArgumentException("Missing value filling policy not correct!");
						}
						break;
					case STRING:
						if ("str_type_empty".equals(fillValue)) {
							r.setField(idx, "");
						} else {
							r.setField(idx, fillValue);
						}
						break;
					default:
						throw new NoSuchMethodException("Unsupported type!");
				}
			} else {
				r.setField(idx, row.getField(selectedColIndices[idx]));
			}
		}

		return this.predictResultColsHelper.getResultRow(row, r);
	}

}
