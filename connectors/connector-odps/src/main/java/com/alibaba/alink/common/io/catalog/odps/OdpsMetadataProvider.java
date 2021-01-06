package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.util.Preconditions;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OdpsMetadataProvider {
	private static final Logger LOG = LoggerFactory.getLogger(OdpsMetadataProvider.class);
	private static final Map <String, Project> projectCache = new HashMap<>();
	private static final Map<String, Table> tableCache = new HashMap<>();
	private static final Map<String, String[]> allPartitionCache = new HashMap<>();
	private static final Map<String, Partition> partitionCache = new HashMap<>();

	public static Project getProject(Odps odps, String project) throws OdpsException {
		if (projectCache.containsKey(project)) {
			return projectCache.get(project);
		} else {
			long start = System.currentTimeMillis();
			Project newProject = odps.projects().get(project);
			newProject.reload();
			if (LOG.isDebugEnabled()) {
				long end = System.currentTimeMillis();
				LOG.debug(String.format("get project %s cost %s ms", project, end - start));
			}

			projectCache.put(project, newProject);
			return newProject;
		}
	}

	public static Table getTable(Odps odps, String project, String table) throws OdpsException {
		String cacheKey = project + "." + table;
		if (tableCache.containsKey(cacheKey)) {
			return tableCache.get(cacheKey);
		} else {
			long start = System.currentTimeMillis();
			Table t = odps.tables().get(project, table);
			t.reload();
			if (LOG.isDebugEnabled()) {
				long end = System.currentTimeMillis();
				LOG.debug(String.format("get table %s cost %s ms", cacheKey, end - start));
			}

			tableCache.put(cacheKey, t);
			return t;
		}
	}

	public static String[] getAllPartitions(Odps odps, String project, String table) throws OdpsException {
		String cacheKey = project + "." + table;
		if (allPartitionCache.containsKey(cacheKey)) {
			return allPartitionCache.get(cacheKey);
		} else {
			Table t = getTable(odps, project, table);
			long start = System.currentTimeMillis();
			List <Partition> allPartitions = t.getPartitions();
			if (LOG.isDebugEnabled()) {
				long end = System.currentTimeMillis();
				LOG.debug(String.format("get %s all partitions cost %s ms", cacheKey, end - start));
			}

			String[] partitions = new String[allPartitions.size()];

			for(int i = 0; i < partitions.length; ++i) {
				partitions[i] = allPartitions.get(i).getPartitionSpec().toString();
			}

			allPartitionCache.put(cacheKey, partitions);
			return partitions;
		}
	}

	public static Partition getPartition(Odps odps, String project, String table, PartitionSpec partitionSpec) throws OdpsException {
		String cacheKey = project + "." + table + ":" + partitionSpec.toString();
		if (partitionCache.containsKey(cacheKey)) {
			return partitionCache.get(cacheKey);
		} else {
			Table t = getTable(odps, project, table);
			Preconditions.checkArgument(t.isPartitioned(), "The table is not a partitioned table!");
			long start = System.currentTimeMillis();
			Partition p = t.getPartition(partitionSpec);
			p.reload();
			if (LOG.isDebugEnabled()) {
				long end = System.currentTimeMillis();
				LOG.debug(String.format("get partition %s cost %s ms", cacheKey, end - start));
			}

			partitionCache.put(cacheKey, p);
			return p;
		}
	}

	public static DownloadSession createDownloadSession(TableTunnel tunnel, String project, String table) {
		String sessionName = project + "." + table;
		long start = System.currentTimeMillis();
		DownloadSession downloadSession = retryableCreateDownLoadSession(tunnel, project, table, (PartitionSpec)null);
		if (LOG.isDebugEnabled()) {
			long end = System.currentTimeMillis();
			LOG.debug(String.format("create %s download session cost %s ms", sessionName, end - start));
		}

		return downloadSession;
	}

	public static DownloadSession createDownloadSession(TableTunnel tunnel, String project, String table, PartitionSpec partitionSpec) {
		Preconditions.checkNotNull(partitionSpec);
		String sessionName = project + "." + table + ":" + partitionSpec.toString();
		long start = System.currentTimeMillis();
		DownloadSession downloadSession = retryableCreateDownLoadSession(tunnel, project, table, partitionSpec);
		if (LOG.isDebugEnabled()) {
			long end = System.currentTimeMillis();
			LOG.debug(String.format("create %s download session cost %s ms", sessionName, end - start));
		}

		return downloadSession;
	}

	private static DownloadSession retryableCreateDownLoadSession(TableTunnel tunnel, String project, String table, PartitionSpec partitionSpec) {
		int tryTimes = 0;

		while(true) {
			try {
				DownloadSession downloadSession;
				if (partitionSpec == null) {
					downloadSession = tunnel.createDownloadSession(project, table);
				} else {
					downloadSession = tunnel.createDownloadSession(project, table, partitionSpec);
				}

				return downloadSession;
			} catch (TunnelException ex) {
				String downloadObjectStr;
				if (partitionSpec == null) {
					downloadObjectStr = String.format("odps table [%s].[%s]!", project, table);
				} else {
					downloadObjectStr = String.format("partition [%s] of odps table [%s].[%s]!", partitionSpec.toString(), project, table);
				}

				if (tryTimes++ >= 5) {
					logAndPropagateException(ex, "Give up to create download session of %s after try %d times", downloadObjectStr, 5);
				} else {
					LOG.warn("Retry to create download session of {} after try {} times", downloadObjectStr, tryTimes, ex);

					try {
						TimeUnit.SECONDS.sleep(tryTimes);
					} catch (InterruptedException interruptedException) {
						logAndPropagateException(interruptedException, "Stop to create download session of %s because of interruption", downloadObjectStr);
					}
				}
			}
		}
	}

	private static void logAndPropagateException(Throwable t, String format, Object... arguments) {
		String warnMsg = String.format(format, arguments);
		LOG.warn(warnMsg, t);
		throw new RuntimeException(warnMsg, t);
	}

	private OdpsMetadataProvider() {
	}
}
