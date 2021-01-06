package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsTableUtil {

	public static String[] getColNames(List <Column> list) {
		int size = list.size();
		String[] colNames = new String[size];

		for (int i = 0; i < size; i++) {
			colNames[i] = list.get(i).getName();
		}

		return colNames;
	}

	public static TypeInformation <?>[] getColTypes(List <Column> list) {
		int size = list.size();
		TypeInformation <?>[] colTypes = new TypeInformation <?>[size];
		for (int i = 0; i < size; i++) {
			OdpsType odpsType = list.get(i).getTypeInfo().getOdpsType();
			colTypes[i] = ODPS_TYPE_TO_FLINK_MAP.get(odpsType);
			if (colTypes[i] == null) {
				throw new IllegalStateException("OdpsType[" + odpsType + "is not support yet!");
			}
		}
		return colTypes;
	}

	public static Column createOdpsColumn(String name, TypeInformation <?> type) {
		OdpsType t = FLINK_TYPE_TO_ODPS_MAP.get(type);
		if (t == null) {
			throw new IllegalStateException("Type[" + type + "] can not converted to OdpsType.");
		}
		return new Column(name, t);
	}

	public static Row odpsRecord2Row(Record record) {
		int colNum = record.getColumnCount();
		Row rows = new Row(colNum);
		for (int i = 0; i < colNum; i++) {
			if (record.getColumns()[i].getType().equals(OdpsType.STRING)) {
				rows.setField(i, record.getString(i));
			} else {
				rows.setField(i, record.get(i));
			}
		}
		return rows;
	}

	static final Map <OdpsType, TypeInformation <?>> ODPS_TYPE_TO_FLINK_MAP = new HashMap <>();
	static final Map <TypeInformation <?>, OdpsType> FLINK_TYPE_TO_ODPS_MAP = new HashMap <>();

	static {
		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.BOOLEAN, Types.BOOLEAN());
		FLINK_TYPE_TO_ODPS_MAP.put(Types.BOOLEAN(), OdpsType.BOOLEAN);

		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.BIGINT, Types.LONG());
		FLINK_TYPE_TO_ODPS_MAP.put(Types.LONG(), OdpsType.BIGINT);

		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.DOUBLE, Types.DOUBLE());
		FLINK_TYPE_TO_ODPS_MAP.put(Types.DOUBLE(), OdpsType.DOUBLE);

		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.STRING, Types.STRING());
		FLINK_TYPE_TO_ODPS_MAP.put(Types.STRING(), OdpsType.STRING);

		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.DATETIME, Types.SQL_TIMESTAMP());
		FLINK_TYPE_TO_ODPS_MAP.put(Types.SQL_TIMESTAMP(), OdpsType.DATETIME);

		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.TINYINT, Types.BYTE());
		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.SMALLINT, Types.SHORT());
		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.INT, Types.INT());
		ODPS_TYPE_TO_FLINK_MAP.put(OdpsType.FLOAT, Types.FLOAT());
	}

}
