package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.dataproc.HasTargetType;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;

import java.util.Arrays;
import java.util.function.Function;

/**
 *
 */
public class NumericalTypeCastMapper extends Mapper {
	private final OutputColsHelper outputColsHelper;
	private final int[] colIndices;
	private final TypeInformation targetType;
	private transient Function castFunc;

	public NumericalTypeCastMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String[] inputColNames = this.params.get(NumericalTypeCastParams.SELECTED_COLS);
		this.colIndices = TableUtil.findColIndices(dataSchema.getFieldNames(), inputColNames);
		String[] outputColNames = params.get(NumericalTypeCastParams.OUTPUT_COLS);
		if (outputColNames == null || outputColNames.length == 0) {
			outputColNames = inputColNames;
		}

		String[] reservedColNames = params.get(NumericalTypeCastParams.RESERVED_COLS);

		if (reservedColNames == null || reservedColNames.length == 0) {
			reservedColNames = dataSchema.getFieldNames();
		}

		targetType = FlinkTypeConverter.getFlinkType(params.get(HasTargetType.TARGET_TYPE));

		this.outputColsHelper = new OutputColsHelper(dataSchema, outputColNames,
			Arrays.stream(outputColNames).map(x -> targetType).toArray(TypeInformation[]::new), reservedColNames);
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	private void initCastFunc() {
		if (targetType.equals(Types.DOUBLE)) {
			castFunc = x -> ((Number) x).doubleValue();
		} else if (targetType.equals(Types.LONG)) {
			castFunc = x -> ((Number) x).longValue();
		} else if (targetType.equals(Types.INT)) {
			castFunc = x -> ((Number) x).intValue();
		} else {
			throw new RuntimeException("Unsupported target type:" + targetType.getTypeClass().getCanonicalName());
		}
	}

	@Override
	public Row map(Row row) throws Exception {
        // this initiate operation should be in open() method. But in somewhere else like TreeModelMapper, this mapper
        // is used directly instead of in flink, so the open() in RichFunction doesn't work. May fix later.
		if (castFunc ==  null) {
			initCastFunc();
		}
		return this.outputColsHelper.getResultRow(row,
			Row.of(Arrays.stream(this.colIndices)
				.mapToObj(row::getField)
				.map(x -> x == null ? null : castFunc.apply(x))
				.toArray(Object[]::new)
			)
		);
	}
}
