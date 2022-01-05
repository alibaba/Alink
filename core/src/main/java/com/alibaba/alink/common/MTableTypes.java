package com.alibaba.alink.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.common.collect.HashBiMap;

/**
 * Built-in mTable types. <br/>
 * This class contains bi-direction mapping between <code>TypeInformation</code>s and their names.
 */
public class MTableTypes {
	private static final HashBiMap <String, TypeInformation> TYPES = HashBiMap.create();

	/**
	 * <code>MTable</code> type information.
	 */
	public static final TypeInformation <MTable> M_TABLE = TypeInformation.of(MTable.class);

	static {
		TYPES.put("M_TABLE", M_TABLE);
	}

	/**
	 * Get type name from <code>TypeInformation</code>.
	 *
	 * @param type <code>TypeInformation</code>
	 * @return Corresponding type name, or null if  not found.
	 */
	public static String getTypeName(TypeInformation type) {
		return TYPES.inverse().get(type);
	}

	/**
	 * Get <code>TypeInformation</code> from type name.
	 *
	 * @param name type name string.
	 * @return Corresponding <code>TypeInformation</code>, or null if not found.
	 */
	public static TypeInformation getTypeInformation(String name) {
		return TYPES.get(name);
	}

	public static boolean isMTableType(TypeInformation <?> type) {
		return type.equals(M_TABLE);
	}
}
