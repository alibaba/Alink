package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.params.mapper.MISOMapperParams;

/**
 * Mapper with Multi-Input columns and Single Output column(MISO).
 */
public abstract class MISOMapper extends Mapper {
	private static final long serialVersionUID = 7808362775563479371L;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input table schema.
	 * @param params     input parameters.
	 */
	public MISOMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	/**
	 * Determine the return type of the {@link MISOMapper#map(Object[])}
	 *
	 * @return the output column type.
	 */
	protected abstract TypeInformation<?> initOutputColType();

	/**
	 * Map input objects to single object.
	 *
	 * @param input input objects.
	 * @return output object.
	 */
	protected abstract Object map(Object[] input) throws Exception;

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Object[] input = new Object[selection.length()];
		for (int i = 0; i < selection.length(); i++) {
			input[i] = selection.get(i);
		}

		result.set(0, map(input));
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {
		String[] keepColNames = null;
		if (params.contains(MISOMapperParams.RESERVED_COLS)) {
			keepColNames = params.get(MISOMapperParams.RESERVED_COLS);
		}

		return Tuple4.of(
			params.get(MISOMapperParams.SELECTED_COLS),
			new String[] {params.get(MISOMapperParams.OUTPUT_COL)},
			new TypeInformation<?>[] {initOutputColType()},
			keepColNames
		);
	}
}
