package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.format.FromColumnsParams;
import com.alibaba.alink.params.dataproc.format.FromCsvParams;
import com.alibaba.alink.params.dataproc.format.FromJsonParams;
import com.alibaba.alink.params.dataproc.format.FromKvParams;
import com.alibaba.alink.params.dataproc.format.FromVectorParams;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError;
import com.alibaba.alink.params.dataproc.format.ToColumnsParams;
import com.alibaba.alink.params.dataproc.format.ToCsvParams;
import com.alibaba.alink.params.dataproc.format.ToJsonParams;
import com.alibaba.alink.params.dataproc.format.ToKvParams;
import com.alibaba.alink.params.dataproc.format.ToVectorParams;
import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class FormatTransMapper extends Mapper {

	private static final long serialVersionUID = 1593086924063348568L;

	private boolean isError;
	private boolean toFillNull;
	private int outputSize;

	private transient ThreadLocal<Row> inputBufferThreadLocal;
	private transient ThreadLocal<FormatReader> formatReaderThreadLocal;
	private transient ThreadLocal<FormatWriter> formatWriterThreadLocal;


	/**
	 * Constructor.
	 *
	 * @param dataSchema the dataSchema.
	 * @param params     the params.
	 */
	public FormatTransMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}


	@Override
	public void open() {
		Tuple2 <FormatReader, String[]> t2From = initFormatReader(super.getDataSchema(), params);
		formatReaderThreadLocal = ThreadLocal.withInitial(() -> initFormatReader(super.getDataSchema(), params).f0);
		String[] fromColNames = t2From.f1;

		formatWriterThreadLocal = ThreadLocal.withInitial(() -> initFormatWriter(params, fromColNames).f0);
		inputBufferThreadLocal = ThreadLocal.withInitial(() -> new Row(ioSchema.f0.length));
	}

	static Tuple2 <FormatReader, String[]> initFormatReader(TableSchema dataSchema, Params params) {
		FormatReader formatReader;
		String[] fromColNames;
		HasHandleInvalidDefaultAsError.HandleInvalid handleInvalid = params
			.get(HasHandleInvalidDefaultAsError.HANDLE_INVALID);
		FormatType fromFormat = params.get(FormatTransParams.FROM_FORMAT);
		switch (fromFormat) {
			case KV:
				String kvColName = params.get(FromKvParams.KV_COL);
				int kvColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), kvColName);
				formatReader = new KvReader(
					kvColIndex,
					params.get(FromKvParams.KV_COL_DELIMITER),
					params.get(FromKvParams.KV_VAL_DELIMITER)
				);
				fromColNames = null;
				break;
			case CSV:
				String csvColName = params.get(FromCsvParams.CSV_COL);
				int csvColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), csvColName);
				TableSchema fromCsvSchema = TableUtil.schemaStr2Schema(params.get(FromCsvParams.SCHEMA_STR));
				formatReader = new CsvReader(
					csvColIndex,
					fromCsvSchema,
					params.get(FromCsvParams.CSV_FIELD_DELIMITER),
					params.get(FromCsvParams.QUOTE_CHAR)
				);
				fromColNames = fromCsvSchema.getFieldNames();
				break;
			case VECTOR:
				String vectorColName = params.get(FromVectorParams.VECTOR_COL);
				int vectorColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(),
					vectorColName);
				if (params.contains(HasSchemaStr.SCHEMA_STR)) {
					formatReader = new VectorReader(
						vectorColIndex,
						TableUtil.schemaStr2Schema(params.get(HasSchemaStr.SCHEMA_STR)),
						handleInvalid
					);
				} else {
					formatReader = new VectorReader(vectorColIndex, null, handleInvalid);
				}
				fromColNames = null;
				break;
			case JSON:
				String jsonColName = params.get(FromJsonParams.JSON_COL);
				int jsonColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), jsonColName);
				formatReader = new JsonReader(jsonColIndex);
				fromColNames = null;
				break;
			case COLUMNS:
				fromColNames = params.get(FromColumnsParams.SELECTED_COLS);
				if (null == fromColNames) {
					fromColNames = dataSchema.getFieldNames();
				}
				int[] colIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema.getFieldNames(), fromColNames);
				formatReader = new ColumnsReader(colIndices, fromColNames);
				break;
			default:
				throw new AkUnsupportedOperationException("translate input type unsupported: " + fromFormat);
		}

		return new Tuple2 <>(formatReader, fromColNames);
	}

	public static Tuple3 <FormatWriter, String[], TypeInformation[]> initFormatWriter(Params params,
																					  String[] fromColNames) {
		FormatType toFormat = params.get(FormatTransParams.TO_FORMAT);
		FormatWriter formatWriter;
		String[] outputColNames;
		TypeInformation[] outputColTypes;

		switch (toFormat) {
			case COLUMNS:
				TableSchema schema = TableUtil.schemaStr2Schema(params.get(ToColumnsParams.SCHEMA_STR));
				formatWriter = new ColumnsWriter(schema);
				outputColNames = schema.getFieldNames();
				outputColTypes = schema.getFieldTypes();
				break;
			case JSON:
				formatWriter = new JsonWriter();
				outputColNames = new String[] {params.get(ToJsonParams.JSON_COL)};
				outputColTypes = new TypeInformation[] {Types.STRING};
				break;
			case KV:
				formatWriter = new KvWriter(
					params.get(ToKvParams.KV_COL_DELIMITER),
					params.get(ToKvParams.KV_VAL_DELIMITER)
				);
				outputColNames = new String[] {params.get(ToKvParams.KV_COL)};
				outputColTypes = new TypeInformation[] {Types.STRING};
				break;
			case CSV:
				formatWriter = new CsvWriter(
					TableUtil.schemaStr2Schema(params.get(ToCsvParams.SCHEMA_STR)),
					params.get(ToCsvParams.CSV_FIELD_DELIMITER),
					params.get(ToCsvParams.QUOTE_CHAR)
				);
				outputColNames = new String[] {params.get(ToCsvParams.CSV_COL)};
				outputColTypes = new TypeInformation[] {Types.STRING};
				break;
			case VECTOR:
				formatWriter = new VectorWriter(
					params.get(ToVectorParams.VECTOR_SIZE),
					fromColNames
				);
				outputColNames = new String[] {params.get(ToVectorParams.VECTOR_COL)};
				outputColTypes = new TypeInformation[] {AlinkTypes.VECTOR};
				break;
			default:
				throw new AkUnsupportedOperationException("translate output type unsupported: " + toFormat);
		}

		return new Tuple3 <>(formatWriter, outputColNames, outputColTypes);

	}

	/**
	 * The operation function to transform vector to table columns.
	 */
	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Row inputBuffer = inputBufferThreadLocal.get();
		selection.fillRow(inputBuffer);
		Map <String, String> bufMap = new HashMap <>();
		boolean success = formatReaderThreadLocal.get().read(inputBuffer, bufMap);
		if (!success) {
			if (isError) {
				throw new AkIllegalDataException("Illegal input data:" + inputBuffer);
			} else {
				for (int i = 0; i < outputSize; i++) {
					result.set(i, null);
				}
				return;
			}
		}
		Tuple2 <Boolean, Row> resultData = formatWriterThreadLocal.get().write(bufMap);
		if (!resultData.f0) {
			if (isError) {
				throw new AkIllegalDataException("failed to output data:" + JsonConverter.toJson(bufMap));
			} else {
				for (int i = 0; i < outputSize; i++) {
					result.set(i, null);
				}
				return;
			}
		}
		if (toFillNull) {
			int length = resultData.f1.getArity();
			for (int i = 0; i < length; i++) {
				if (resultData.f1.getField(i) == null) {
					resultData.f1.setField(i, 0.0);
				}
			}
		}
		for (int i = 0; i < outputSize; i++) {
			result.set(i, resultData.f1.getField(i));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		Tuple2 <FormatReader, String[]> t2From = initFormatReader(dataSchema, params);
		String[] fromColNames = t2From.f1;

		Tuple3 <FormatWriter, String[], TypeInformation[]> t3To = initFormatWriter(params, fromColNames);
		String[] outputColNames = t3To.f1;
		TypeInformation[] outputColTypes = t3To.f2;

		this.isError = params.get(HasHandleInvalidDefaultAsError.HANDLE_INVALID)
			.equals(HasHandleInvalidDefaultAsError.HandleInvalid.ERROR);
		this.toFillNull = params.get(FormatTransParams.FROM_FORMAT).equals(FormatType.VECTOR) &&
			params.get(FormatTransParams.TO_FORMAT).equals(FormatType.COLUMNS);
		outputSize = outputColNames.length;

		return Tuple4.of(dataSchema.getFieldNames(), outputColNames, outputColTypes,
			params.get(HasReservedColsDefaultAsNull.RESERVED_COLS));

	}

}
