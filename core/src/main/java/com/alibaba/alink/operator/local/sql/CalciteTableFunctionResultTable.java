package com.alibaba.alink.operator.local.sql;

import org.apache.flink.types.Row;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

class CalciteTableFunctionResultTable extends AbstractTable implements ScannableTable {
	private final List <Row> results;
	private final RelDataType rowType;

	public CalciteTableFunctionResultTable(List <Row> results, RelDataType rowType) {
		this.results = results;
		this.rowType = rowType;
	}

	@Override
	public Enumerable <Object[]> scan(DataContext dataContext) {
		return new AbstractEnumerable <Object[]>() {
			@Override
			public Enumerator <Object[]> enumerator() {
				return new Enumerator <Object[]>() {
					int index = -1;
					Object[] current = setCurrent();

					private Object[] setCurrent() {
						if (index < 0 || index >= results.size()) {
							return null;
						}
						Row row = results.get(index);
						if (null == current) {
							current = new Object[row.getArity()];
						}
						for (int i = 0; i < row.getArity(); i += 1) {
							current[i] = row.getField(i);
						}
						return current;

					}

					@Override
					public Object[] current() {
						return current;
					}

					@Override
					public boolean moveNext() {
						if (index < results.size() - 1) {
							index += 1;
							setCurrent();
							return true;
						} else {
							return false;
						}
					}

					@Override
					public void reset() {
						// TODO: ignore for now
					}

					@Override
					public void close() {
					}
				};
			}
		};
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
		return rowType;
	}
}
