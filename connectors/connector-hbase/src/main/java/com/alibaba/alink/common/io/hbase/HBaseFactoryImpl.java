package com.alibaba.alink.common.io.hbase;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.io.HBaseConfigParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseFactoryImpl implements HBaseFactory {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseFactoryImpl.class);

	@Override
	public HBase create(Params params) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", params.get(HBaseConfigParams.ZOOKEEPER_QUORUM));
		conf.set("hbase.rpc.timeout", String.valueOf(params.get(HBaseConfigParams.TIMEOUT)));
		final Connection conn = ConnectionFactory.createConnection(conf);
		final Admin admin = conn.getAdmin();
		return new HBase() {
			@Override
			public void close() throws IOException {
				if (!conn.isClosed()) {
					conn.close();
					admin.close();
					LOG.info("hbase connection close");
				}
			}

			@Override
			public void createTable(String tableName, String... columnFamilies) throws IOException {
				TableName tablename = TableName.valueOf(tableName);
				if (admin.tableExists(tablename)) {
					LOG.info(String.format("hbase table %s already exists!", tableName));
				} else {
					HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
					for (String columnFamliy : columnFamilies) {
						tableDescriptor = tableDescriptor.addFamily(new HColumnDescriptor(columnFamliy));
					}
					admin.createTable(tableDescriptor);
					LOG.info(String.format("hbase table %s has been created", tableName));
				}
			}

			@Override
			public void set(String tableName, String rowKey, String familyName, String column, byte[] data)
				throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Put put = new Put(rowKey.getBytes());
				put.addColumn(familyName.getBytes(), column.getBytes(), data);
				table.put(put);
			}

			public void addColumnToPut(Put put, String familyName, Map <String, byte[]> dataMap) {
				dataMap.forEach((key, value) -> {
					put.addColumn(familyName.getBytes(), key.getBytes(), value);
				});
			}

			@Override
			public void set(String tableName, String rowKey, String familyName, Map <String, byte[]> dataMap) throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Put put = new Put(rowKey.getBytes());
				addColumnToPut(put, familyName, dataMap);
				table.put(put);
			}

			@Override
			public void set(String tableName, String rowKey, List <String> familyNames,
							List <Map <String, byte[]>> dataMap) throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Put put = new Put(rowKey.getBytes());
				if (familyNames.size() != dataMap.size()) {
					LOG.warn(String.format("familyNames size %d not equal with dataMap size %d", familyNames.size(), dataMap.size()));
					return;
				}
				for (int i = 0; i < familyNames.size(); i++) {
					addColumnToPut(put, familyNames.get(i), dataMap.get(i));
				}
				table.put(put);
			}

			@Override
			public byte[] getColumn(String tableName, String rowKey, String familyName, String column) throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Get get = new Get(rowKey.getBytes());
				Result result = table.get(get);
				byte[] resultValue = result.getValue(familyName.getBytes(), column.getBytes());
				return resultValue;
			}

			@Override
			public Map <String, byte[]> getFamilyColumns(String tableName, String rowKey, String familyName) throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Get get = new Get(rowKey.getBytes());
				Result result = table.get(get);
				Map <byte[], byte[]> map = result.getFamilyMap(familyName.getBytes());
				if (null == map) {
					return new HashMap <>();
				}
				Map <String, byte[]> ret = new HashMap <>(map.size());
				for (byte[] key : map.keySet()) {
					ret.put(Bytes.toString(key), map.get(key));
				}
				return ret;
			}

			@Override
			public  Map<byte[], NavigableMap <byte[], byte[]>> getRow(String tableName, String rowKey) throws IOException {
				Table table = conn.getTable(TableName.valueOf(tableName));
				Get get = new Get(rowKey.getBytes());
				Result result = table.get(get);
				Map<byte[], NavigableMap <byte[], byte[]>> resultMap = result.getNoVersionMap();
				return resultMap;
			}
		};
	}
}
