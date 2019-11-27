package com.alibaba.alink.common.io.directreader;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TableNeed2Collection implements Serializable {

	private final static Logger LOG = LoggerFactory.getLogger(TableNeed2Collection.class);

	private List <Params> dbList = new ArrayList <>();

	private TableNeed2Collection() {
	}

	public static TableNeed2Collection getCollectionClient() {
		return CollectionInstance.collection;
	}

	public synchronized void insertTable(Params dbParams) {
		dbList.add(dbParams);
	}

	public synchronized void cleanAll() throws Exception {
		for (Params dbParams : dbList) {
			BaseDB db = BaseDB.of(dbParams);

			String tableName = dbParams.getString(AnnotationUtils.annotatedAlias(db.getClass()));

			db.dropTable(tableName);

			LOG.info("Drop table: {}", tableName);
		}

		dbList.clear();
	}

	private static class CollectionInstance {
		private static final TableNeed2Collection collection = new TableNeed2Collection();
	}

	static {
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						TableNeed2Collection.getCollectionClient().cleanAll();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
