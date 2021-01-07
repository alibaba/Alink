package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.util.Preconditions;

import com.aliyun.odps.Column;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsTableSchema implements Serializable {
	private static final long serialVersionUID = -6327923765714170499L;
	private final List <OdpsColumn> columns;
	private final boolean isPartition;
	private final boolean isView;
	private int partitionColumnNum;
	private transient Map <String, OdpsColumn> columnMap;

	public OdpsTableSchema(List <Column> normalColumns, List <Column> partitionColumns, boolean isView) {
		Preconditions.checkArgument(normalColumns != null && !normalColumns.isEmpty(),
			"input normal columns cannot be null or empty!");
		List <OdpsColumn> columnList = new ArrayList <>();

		for (Column column : normalColumns) {
			columnList.add(new OdpsColumn(column.getName(), column.getType()));
		}

		this.isView = isView;
		boolean hasPartitionCols = partitionColumns != null && !partitionColumns.isEmpty();
		this.partitionColumnNum = 0;
		if (hasPartitionCols) {
			List <OdpsColumn> partitionColumnList = new ArrayList <>();

			for (Column column : partitionColumns) {
				partitionColumnList.add(new OdpsColumn(column.getName(), column.getType(), true));
			}

			columnList.addAll(partitionColumnList);
			this.partitionColumnNum = partitionColumnList.size();
		}

		this.isPartition = !isView && hasPartitionCols;
		this.columns = columnList;
		this.rebuildColumnMap();
	}

	public List <OdpsColumn> getColumns() {
		return this.columns;
	}

	public boolean isPartition() {
		return this.isPartition;
	}

	public boolean isView() {
		return this.isView;
	}

	public OdpsColumn getColumn(String name) {
		return this.columnMap.get(name);
	}

	public boolean isPartitionColumn(String name) {
		OdpsColumn column = this.columnMap.get(name);
		if (column != null) {
			return column.isPartition();
		} else {
			throw new IllegalArgumentException("unknown column " + name);
		}
	}

	private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
		inputStream.defaultReadObject();
		this.rebuildColumnMap();
	}

	private void rebuildColumnMap() {
		Map <String, OdpsColumn> tmpMap = new HashMap <>();

		for (OdpsColumn column : this.columns) {
			if (column != null) {
				tmpMap.put(column.getName(), column);
			}
		}

		this.columnMap = tmpMap;
	}

	public int getPartitionColumnNum() {
		return this.partitionColumnNum;
	}
}