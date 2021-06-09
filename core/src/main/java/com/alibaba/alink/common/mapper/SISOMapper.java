package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * Abstract class for mappers with Single Input column and Single Output column(SISO).
 */
public abstract class SISOMapper extends Mapper {

	private static final long serialVersionUID = 6112286812006547059L;

	/**
	 * Constructor.
	 *
	 * @param dataSchema input tableSchema
	 * @param params     input parameters.
	 */
	public SISOMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	/**
	 * Determine the return type of the {@link SISOMapper#map(Row)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation initOutputColType();

	/**
	 * Map the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single map result.
	 * @throws Exception
	 */
	protected abstract Object mapColumn(Object input) throws Exception;

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		result.set(0, mapColumn(selection.get(0)));
	}

	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		String outputColName = params.get(SISOMapperParams.OUTPUT_COL);
		if (outputColName == null) {
			outputColName = params.get(SISOMapperParams.SELECTED_COL);
		}

		return Tuple4.of(
			new String[] {params.get(SISOMapperParams.SELECTED_COL)},
			new String[] {outputColName},
			new TypeInformation <?>[] {initOutputColType()},
			params.get(SISOMapperParams.RESERVED_COLS)
		);
	}
}
