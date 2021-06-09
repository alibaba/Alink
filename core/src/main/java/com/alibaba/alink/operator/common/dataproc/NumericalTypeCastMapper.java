package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;

import java.util.Arrays;
import java.util.function.Function;

/**
 *
 */
public class NumericalTypeCastMapper extends Mapper {

	private static final long serialVersionUID = 767160752523041431L;

	private final TypeInformation<?> targetType;

	private transient Function<Object, Object> castFunc;

	public NumericalTypeCastMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		targetType = FlinkTypeConverter.getFlinkType(
			params.get(NumericalTypeCastParams.TARGET_TYPE).toString()
		);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		if (castFunc == null) {
			initCastFunc();
		}
		for (int i = 0; i < selection.length(); ++i) {
			result.set(i, castFunc.apply(selection.get(i)));
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		String[] inputColNames = this.params.get(NumericalTypeCastParams.SELECTED_COLS);

		String[] outputColNames = params.get(NumericalTypeCastParams.OUTPUT_COLS);
		if (outputColNames == null || outputColNames.length == 0) {
			outputColNames = inputColNames;
		}

		TypeInformation<?> targetType = FlinkTypeConverter
			.getFlinkType(params.get(NumericalTypeCastParams.TARGET_TYPE).toString());

		TypeInformation<?>[] outputColTypes = Arrays
			.stream(outputColNames)
			.map(x -> targetType)
			.toArray(TypeInformation[]::new);

		String[] reservedColNames = params.get(NumericalTypeCastParams.RESERVED_COLS);

		if (reservedColNames == null || reservedColNames.length == 0) {
			reservedColNames = dataSchema.getFieldNames();
		}

		return Tuple4.of(inputColNames, outputColNames, outputColTypes, reservedColNames);
	}

	private void initCastFunc() {
		if (targetType.equals(Types.DOUBLE)) {
			castFunc = x -> {
				if (x == null) {
					return null;
				} else if (x instanceof String) {
					return Double.parseDouble((String) x);
				} else {
					return ((Number) x).doubleValue();
				}
			};
		} else if (targetType.equals(Types.LONG)) {
			castFunc = x -> {
				if (x == null) {
					return null;
				} else if (x instanceof String) {
					return Long.parseLong((String) x);
				} else {
					return ((Number) x).longValue();
				}
			};
		} else if (targetType.equals(Types.INT)) {
			castFunc = x -> {
				if (x == null) {
					return null;
				} else if (x instanceof String) {
					return Integer.parseInt((String) x);
				} else {
					return ((Number) x).intValue();
				}
			};
		} else if (targetType.equals(Types.FLOAT)) {
			castFunc = x -> {
				if (x == null) {
					return null;
				} else if (x instanceof String) {
					return Float.parseFloat((String) x);
				} else {
					return ((Number) x).floatValue();
				}
			};
		} else {
			throw new RuntimeException("Unsupported target type:" + targetType.getTypeClass().getCanonicalName());
		}
	}
}
