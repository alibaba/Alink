package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for bucketizer.
 */
public interface BucketizerParams<T> extends
	HasOutputColsDefaultAsNull<T>,
	HasReservedCols<T> {

	ParamInfo <String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", String.class)
		.setDescription("parameter for how to handle invalid data (NULL values)")
		.setHasDefaultValue("error")
		.build();

	/**
	 * parameter how to handle invalid data (NaN values). Options are 'skip' (filter out rows with
	 * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the output).
	 */
	default String getHandleInvalid() {return get(HANDLE_INVALID);}

	default T setHandleInvalid(String value) {return set(HANDLE_INVALID, value);}

	ParamInfo <String[]> SELECTED_COLS = ParamInfoFactory
		.createParamInfo("selectedCols", String[].class)
		.setDescription("Names of the columns used for processing")
		.setAlias(new String[] {"selectedColNames"})
		.build();

	default String[] getSelectedCols() {
		return get(SELECTED_COLS);
	}

	default T setSelectedCols(String... value) {
		return set(SELECTED_COLS, value);
	}


	ParamInfo <String[]> SPLITS_ARRAY = ParamInfoFactory
		.createParamInfo("splitsArray", String[].class)
		.setDescription("Split points array, each of them is used for the corresponding selected column.")
		.build();

	default String[] getSplitsArray() {
		return get(SPLITS_ARRAY);
	}

	default T setSplitsArray(String... value) {
		return set(SPLITS_ARRAY, value);
	}

}
