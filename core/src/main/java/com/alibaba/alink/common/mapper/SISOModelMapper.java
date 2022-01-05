package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.params.mapper.SISOMapperParams;

/**
 * ModelMapper with Single Input column and Single Output column(SISO).
 */
public abstract class SISOModelMapper extends ModelMapper {

	private static final long serialVersionUID = -6621260232816861723L;

	public SISOModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	/**
	 * Determine the prediction result type of the {@link SISOModelMapper#predictResult(Object)}.
	 *
	 * @return the outputColType.
	 */
	protected abstract TypeInformation<?> initPredResultColType();

	/**
	 * Predict the single input column <code>input</code> to a single output.
	 *
	 * @param input the input object
	 * @return the single predicted result.
	 * @throws Exception
	 */
	protected abstract Object predictResult(Object input) throws Exception;

	@Override
	protected final Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		String outputColName = params.get(SISOMapperParams.OUTPUT_COL);
		if (outputColName == null) {
			outputColName = params.get(SISOMapperParams.SELECTED_COL);
		}

		return Tuple4.of(
			new String[] {params.get(SISOMapperParams.SELECTED_COL)},
			new String[] {outputColName},
			new TypeInformation<?>[] {initPredResultColType()},
			params.get(SISOMapperParams.RESERVED_COLS)
		);
	}

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		result.set(0, predictResult(selection.get(0)));
	}
}
