package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.BaseDB;

import java.util.List;

/**
 * An DataBridge read data from {@link BaseDB}.
 */
public class DbDataBridge implements DataBridge {
	private Params dbParams;

	public DbDataBridge() {
	}

	public Params getDbParams() {
		return dbParams;
	}

	public DbDataBridge setDbParams(Params dbParams) {
		this.dbParams = dbParams;
		return this;
	}

	@Override
	public List <Row> read(FilterFunction <Row> filter) {
		try {
			return BaseDB
				.of(dbParams)
				.directRead(this, filter);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
