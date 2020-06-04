package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.common.utils.JsonConverter;

import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError.*;
import com.alibaba.alink.params.dataproc.format.ToTripleParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper.initFormatReader;

/**
 */
public class AnyToTripleFlatMapper extends FlatMapper implements Serializable {

	private static final long serialVersionUID = 3221991172531895169L;
	private OutputColsHelper outputColsHelper;
	private HandleInvalid handleInvalid;
	private FormatReader formatReader;

	private HashMap <String, String> bufMap = new HashMap <>();

	private FieldParser <?>[] parsers;
	private boolean[] isString;
	private TypeInformation[] fieldTypes;

	/**
	 * Constructor.
	 *
	 * @param dataSchema the dataSchema.
	 * @param params     the params.
	 */
	public AnyToTripleFlatMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		TableSchema schema = CsvUtil.schemaStr2Schema(params.get(ToTripleParams.TRIPLE_COL_VAL_SCHEMA_STR));

		fieldTypes = schema.getFieldTypes();
		String[] reversedCols = this.params.get(ToTripleParams.RESERVED_COLS);
		this.handleInvalid = params.get(HasHandleInvalidDefaultAsError.HANDLE_INVALID);
		this.outputColsHelper = new OutputColsHelper(
			dataSchema,
			schema.getFieldNames(),
			schema.getFieldTypes(),
			reversedCols
		);
	}

	@Override
	public void open() {
		this.formatReader = initFormatReader(super.getDataSchema(), params).f0;
		this.isString = new boolean[fieldTypes.length];
		this.parsers = new FieldParser[fieldTypes.length];

		for (int i = 0; i < fieldTypes.length; i++) {
			parsers[i] = ColumnsWriter.getFieldParser(fieldTypes[i].getTypeClass());
			isString[i] = fieldTypes[i].equals(Types.STRING);
		}
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		if (null == row) {
			output.collect(null);
		}
		bufMap.clear();
		boolean success = formatReader.read(row, bufMap);
		if (success) {
			for (Map.Entry <String, String> entry : bufMap.entrySet()) {
				Tuple2 <Boolean, Object> parsedKey = ColumnsWriter.parseField(parsers[0], entry.getKey(), isString[0]);
				Tuple2 <Boolean, Object> parsedValue = ColumnsWriter.parseField(parsers[1], entry.getValue(), isString[1]);
				if (!StringUtils.isNullOrWhitespaceOnly(entry.getValue())) {
					if (parsedKey.f0 && parsedValue.f0) {
						output.collect(outputColsHelper
							.getResultRow(row, Row.of(parsedKey.f1, parsedValue.f1)));
					} else if (handleInvalid.equals(HandleInvalid.ERROR)) {
						throw new RuntimeException("Fail to write: " + JsonConverter.toJson(bufMap));
					}
				}
			}
		} else if (handleInvalid.equals(HandleInvalid.ERROR)) {
			throw new RuntimeException("Fail to read: " + row);
		}
	}

	/**
	 * Get the output data schema.
	 */
	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

}
