package com.alibaba.alink.python.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.pyrunner.fn.DataConversionUtils;

@SuppressWarnings("unused")
public class RowTypeAdapter {

	/**
	 * Adjust fields in the row to given types in-place.
	 * <p>
	 * For example, a field is {@link Integer}, but the given type is {@link org.apache.flink.api.common.typeinfo.Types#LONG}.
	 *
	 * @param row        a {@link Row} instance.
	 * @param fieldTypes filed types.
	 */
	@SuppressWarnings("unused")
	public static void adjustRowTypeInplace(Row row, TypeInformation <?>[] fieldTypes) {
		for (int k = 0; k < row.getArity(); k += 1) {
			row.setField(k, DataConversionUtils.pyToJava(row.getField(k), fieldTypes[k]));
		}
	}

	/**
	 * Check whether the fields in the row match given types. Throw exceptions if not matched.
	 *
	 * @param row        a {@link Row} instance.
	 * @param fieldTypes filed types.
	 */
	@SuppressWarnings("unused")
	public static void checkRowType(Row row, TypeInformation <?>[] fieldTypes) {
		for (int k = 0; k < row.getArity(); k += 1) {
			Object field = row.getField(k);
			assert null != field;
			if (!fieldTypes[k].getTypeClass().isAssignableFrom(field.getClass())) {
				throw new RuntimeException(String.format(
					"Value type is not equal or a subclass of specific field type at %d: %s - %s", k,
					field.getClass().getName(),
					fieldTypes[k].getTypeClass().getName()));
			}
		}
	}
}
