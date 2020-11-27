package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.utils.Functional;

/**
 * Some types defined for Binning.
 */
public class BinTypes {

	public enum ColType {
		/**
		 * Long type, for Int/Short/Byte/Long data.
		 */
		INT(Object::toString, true),

		/**
		 * String type, for String/Boolean/Char.
		 */
		STRING(Object::toString, false),

		/**
		 * Double type, for Double/Float.
		 */
		FLOAT(Object::toString, true);

		final Functional.SerializableFunction <Object, String> objToStrFunc;
		final public boolean isNumeric;

		ColType(Functional.SerializableFunction <Object, String> objToStrFunc,
				boolean isNumeric) {
			this.objToStrFunc = objToStrFunc;
			this.isNumeric = isNumeric;
		}

		public static ColType valueOf(TypeInformation <?> type) {
			if (type.equals(Types.DOUBLE) || type.equals(Types.FLOAT)) {
				return FLOAT;
			} else if (type.equals(Types.LONG) || type.equals(Types.INT) || type.equals(Types.SHORT) || type.equals(
				Types.BYTE)) {
				return INT;
			} else if (type.equals(Types.STRING) || type.equals(Types.BOOLEAN) || type.equals(Types.CHAR)) {
				return STRING;
			} else {
				throw new IllegalArgumentException("Unsupported type: " + type);
			}
		}
	}

}
