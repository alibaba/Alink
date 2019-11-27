package com.alibaba.alink.common.io.table;

import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.DBSourceBatchOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public abstract class BaseDbTable {

	public List <String> listPartitionString() {
		return new ArrayList <>();
	}

	public String getComment() {
		return "";
	}

	public String getOwner() {
		return "";
	}

	public Date getCreatedTime() {
		return null;
	}

	public Date getLastDataModifiedTime() {
		return null;
	}

	public long getLife() {
		return -1;
	}

	public abstract String getTableName();

	public String[] getColComments() {
		String[] comments = new String[this.getColNames().length];
		Arrays.fill(comments, "");
		return comments;
	}

	public abstract TableSchema getSchema();

	public abstract String[] getColNames();

	public abstract Class[] getColTypes();

	public int getColNum() {
		return getColNames().length;
	}

	public long getRowNum() throws Exception {
		DBSourceBatchOp op = new DBSourceBatchOp(getDB(), this.getTableName());
		return op.count();
	}

	public abstract BaseDB getDB();

	public BatchOperator getBatchOperator(Long sessionId) throws Exception {
		return new 	DBSourceBatchOp(this.getDB(), this.getTableName()).setMLEnvironmentId(sessionId);
	}

	public BatchOperator getBatchOperator() throws Exception {
		return getBatchOperator(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}
}
