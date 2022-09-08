package com.alibaba.alink.operator.common.sql;

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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import java.util.Arrays;

class MTableTable extends AbstractTable implements ScannableTable {
	private final MTable mTable;

	public MTableTable(MTable mTable) {
		this.mTable = mTable;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
		String[] names = mTable.getColNames();
		final JavaTypeFactory typeFactory = (JavaTypeFactory) relDataTypeFactory;
		RelDataType[] types = Arrays.stream(mTable.getColTypes())
			.map(d -> SqlTypeUtil.addCharsetAndCollation(typeFactory.createJavaType(d.getTypeClass()), typeFactory))
			.toArray(RelDataType[]::new);
		return typeFactory.createStructType(Pair.zip(names, types));
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
			Object[] objects = new Object[row.getArity()];
			for (int i = 0; i < row.getArity(); i += 1) {
				objects[i] = row.getField(i);
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
