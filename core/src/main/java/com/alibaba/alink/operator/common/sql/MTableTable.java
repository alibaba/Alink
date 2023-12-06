package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.sql.Timestamp;

class MTableTable extends AbstractTable implements ScannableTable {
	private final MTable mTable;

	public MTableTable(MTable mTable) {
		this.mTable = mTable;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
		String[] names = mTable.getColNames();

		int tsCnt = 0;
		for (int i = 0; i < names.length; i++) {
			if (mTable.getColTypes()[i] == Types.SQL_TIMESTAMP) {
				tsCnt++;
			}
		}

		TypeInformation <?>[] types = mTable.getColTypes();
		final JavaTypeFactory typeFactory = (JavaTypeFactory) relDataTypeFactory;

		String[] newNames = new String[names.length + tsCnt];
		RelDataType[] newTypes = new RelDataType[newNames.length];

		int idx = names.length;
		for (int i = 0; i < names.length; i++) {
			newNames[i] = names[i];
			newTypes[i] = typeFactory.createJavaType(types[i].getTypeClass());
			if (mTable.getColTypes()[i] == Types.SQL_TIMESTAMP) {
				newNames[idx] = names[i] + "__ak_ts__";
				newTypes[idx] = typeFactory.createJavaType(types[i].getTypeClass());
				idx++;
			}
		}

		return typeFactory.createStructType(Pair.zip(newNames, newTypes));
	}

	@Override
	public Enumerable <Object[]> scan(DataContext dataContext) {
		return new AbstractEnumerable <Object[]>() {
			@Override
			public Enumerator <Object[]> enumerator() {
				return new MTableEnumerator(mTable);
			}
		};
	}

	static class MTableEnumerator implements Enumerator <Object[]> {
		private final MTable mTable;
		private final int length;
		private int current = -1;

		public MTableEnumerator(MTable mTable) {
			this.mTable = mTable;
			length = mTable.getNumRow();
		}

		@Override
		public Object[] current() {
			Row row = mTable.getRow(current);
			int colNum = row.getArity();
			int tsCnt = 0;
			for (int i = 0; i < row.getArity(); i++) {
				if (mTable.getColTypes()[i] == Types.SQL_TIMESTAMP) {
					tsCnt++;
				}
			}
			Object[] objects = new Object[row.getArity() + tsCnt];

			int idx = colNum;
			for (int i = 0; i < row.getArity(); i += 1) {
				if (mTable.getColTypes()[i] == Types.SQL_TIMESTAMP) {
					objects[idx] = row.getField(i);
					objects[i] = ((Timestamp) row.getField(i)).getTime();
					idx++;
				} else {
					objects[i] = row.getField(i);
				}
			}
			return objects;
		}

		@Override
		public boolean moveNext() {
			if (current < length - 1) {
				++current;
				return true;
			}
			return false;
		}

		@Override
		public void reset() {
			current = 0;
		}

		@Override
		public void close() {
		}
	}
}
