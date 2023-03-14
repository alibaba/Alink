package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class String2NgramRow extends TableFunction <Row> {
	private static final long serialVersionUID = -466143644625699898L;
	private int n = 5;

	public String2NgramRow() {
	}

	public String2NgramRow(int n) {
		this.n = n;
	}

	public void eval(String str) {
		try {
			if (null == str || "" == str) {
				Row r = new Row(2);
				r.setField(0, "");
				r.setField(1, 0);
				collect(r);
				return;
			}
			int length = str.length();
			for (int i = 0; i < length - n + 1; i++) {
				Row r = new Row(2);
				r.setField(0, str.substring(i, i + n));
				r.setField(1, length);
				collect(r);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@Override
	public TypeInformation <Row> getResultType() {
		return new RowTypeInfo(new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO});
	}
}
