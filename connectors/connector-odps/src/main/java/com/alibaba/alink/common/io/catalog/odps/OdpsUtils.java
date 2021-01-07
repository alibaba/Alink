package com.alibaba.alink.common.io.catalog.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OdpsUtils {
	private static final Logger LOG = LoggerFactory.getLogger(OdpsUtils.class);

	public OdpsUtils() {
	}

	public static TableSchema getTableSchema(Odps odps, String project, String table) {
		odps.setDefaultProject(project);
		return odps.tables().get(table).getSchema();
	}

	public static void dealTruncate(Odps odps, String projectName, String tableName, String partition,
									boolean truncate) {
		Table table = odps.tables().get(projectName, tableName);
		boolean isPartitionedTable = isPartitionedTable(table);
		if (isPartitionedTable) {
			if (StringUtils.isEmpty(partition)) {
				throw new RuntimeException(
					String.format("Partitioned table:%s , partition should not be empty.", table.getName()));
			}

			if (truncate) {
				truncatePartition(table, partition);
			} else {
				addPart(table, partition);
			}
		} else {
			if (!StringUtils.isEmpty(partition)) {
				throw new RuntimeException(
					String.format("None paritioned table:%s , the partition should be empty.", table.getName()));
			}

			if (truncate) {
				truncateNonPartitionedTable(table);
			}
		}
	}

	public static boolean isPartitionedTable(Table table) {
		try {
			return table.isPartitioned();
		} catch (Exception ex) {
			throw new RuntimeException(
				String.format("Check table:%s whether is partitioned table failed.", table.getName()), ex);
		}
	}

	private static boolean isPartitionExist(Table table, String partition) {
		try {
			return table.hasPartition(new PartitionSpec(partition));
		} catch (Exception ex) {
			throw new RuntimeException(String.format("Check table:%s partition existence failed.", table.getName()));
		}
	}

	public static void truncatePartition(Table table, String partition) {
		LOG.info("Truncate table: " + table.getName() + " partition: " + partition);
		dropPart(table, partition);
		addPart(table, partition);
	}

	public static void truncateNonPartitionedTable(Table table) {
		try {
			table.truncate();
		} catch (Exception ex) {
			throw new RuntimeException(
				String.format("Truncate table: %s failed.", table.getName()),
				ex
			);
		}
	}

	private static void dropPart(Table table, String partition) {
		try {
			table.deletePartition(new PartitionSpec(partition), true);
		} catch (Exception ex) {
			throw new RuntimeException(
				String.format(
					"Drop partition failed, projectName:%s tableName:%s partition:%s.",
					table.getProject(), table.getName(), partition),
				ex
			);
		}
	}

	public static void addPart(Table table, String partition) {
		try {
			table.createPartition(new PartitionSpec(partition), true);
		} catch (Exception ex) {
			throw new RuntimeException(
				String.format("Add parition failed, projectName:%s tableName:%s partition:%s.",
					table.getProject(), table.getName(), partition
				),
				ex
			);
		}
	}

	public static Odps initOdps(OdpsConf odpsConf) {
		return initOdps(odpsConf.getAccessId(), odpsConf.getAccessKey(), odpsConf.getEndpoint(),
			odpsConf.getProject());
	}

	public static Odps initOdps(String accessId, String accessKey, String endpoint, String defaultProject) {
		Account account = new AliyunAccount(accessId, accessKey);
		Odps odps = new Odps(account);
		odps.setEndpoint(endpoint);
		if (defaultProject != null) {
			odps.setDefaultProject(defaultProject);
		}

		return odps;
	}

	public static OdpsTableSchema getODPSTableSchema(Odps odps, String table) throws IOException {
		try {
			Table t = OdpsMetadataProvider.getTable(odps, odps.getDefaultProject(), table);
			TableSchema schema = t.getSchema();
			boolean isView = t.isVirtualView();
			return new OdpsTableSchema(schema.getColumns(), schema.getPartitionColumns(), isView);
		} catch (OdpsException var5) {
			throw new IOException("get table schema failed", var5);
		}
	}

	public static long getTableSize(OdpsConf odpsConf, String table) throws IOException {
		Odps odps = initOdps(odpsConf);

		try {
			Table t = OdpsMetadataProvider.getTable(odps, odpsConf.getProject(), table);
			return t.getSize();
		} catch (OdpsException var4) {
			throw new IOException("get table size failed", var4);
		}
	}

	public static long getPartitionSize(OdpsConf odpsConf, String table, String partition) throws IOException {
		Odps odps = initOdps(odpsConf);
		PartitionSpec spec = new PartitionSpec(partition);

		try {
			Partition p = OdpsMetadataProvider.getPartition(odps, odpsConf.getProject(), table, spec);
			return p.getSize();
		} catch (OdpsException var6) {
			throw new IOException("get partition size failed", var6);
		}
	}

	public static long getTableLastModifyTime(OdpsConf odpsConf, String table) throws IOException {
		Odps odps = initOdps(odpsConf);

		try {
			Table t = OdpsMetadataProvider.getTable(odps, odpsConf.getProject(), table);
			return t.getLastDataModifiedTime().getTime();
		} catch (OdpsException var4) {
			throw new IOException("get table last modify time failed", var4);
		}
	}

	public static long getPartitionLastModifyTime(OdpsConf odpsConf, String table, String partition)
		throws IOException {
		Odps odps = initOdps(odpsConf);
		PartitionSpec spec = new PartitionSpec(partition);

		try {
			Partition p = OdpsMetadataProvider.getPartition(odps, odpsConf.getProject(), table, spec);
			return p.getLastDataModifiedTime().getTime();
		} catch (OdpsException ex) {
			throw new IOException("get partition last modify time failed", ex);
		}
	}

	public static List <Partition> matchPartitions(List <Partition> partitions, List <String> regexPartitions) {
		List <Pattern> regexPatterns = new ArrayList <>();

		for (String input : regexPartitions) {
			if (input.startsWith("regex:")) {
				input = input.substring(6);
			}

			input = input.replaceAll("\\*", "([\\\\w\\\\W]*)");
			regexPatterns.add(Pattern.compile(input));
		}

		return matchPatterns(partitions, regexPatterns);
	}

	private static List <Partition> matchPatterns(List <Partition> partitions, List <Pattern> regexPatterns) {
		List <Partition> result = new ArrayList <>();

		for (Partition partition : partitions) {
			for (Pattern pattern : regexPatterns) {
				Matcher matcher = pattern.matcher(partition.getPartitionSpec().toString().replaceAll("'", ""));
				if (matcher.matches()) {
					result.add(partition);
					break;
				}
			}
		}

		return result;
	}

	public static String[] getAllPartitions(OdpsConf odpsConf, String table) throws IOException {
		Odps odps = initOdps(odpsConf);

		try {
			return OdpsMetadataProvider.getAllPartitions(odps, odpsConf.getProject(), table);
		} catch (OdpsException ex) {
			throw new IOException("get table all partitions failed", ex);
		}
	}

	public static void createTable(OdpsConf odpsConf, String project, String table, OdpsTableSchema schema,
								   String comment, boolean ifNotExists) throws RuntimeException {
		Odps odps = initOdps(odpsConf);
		TableSchema theSchema = new TableSchema();
		List <OdpsColumn> columns = schema.getColumns();

		for (OdpsColumn column : columns) {
			Column theColumn = new Column(column.getName(), column.getType());
			if (column.isPartition()) {
				theSchema.addPartitionColumn(theColumn);
			} else {
				theSchema.addColumn(theColumn);
			}
		}

		try {
			odps.tables().create(project, table, theSchema, comment, ifNotExists);
		} catch (OdpsException var12) {
			throw new RuntimeException(var12);
		}
	}

	public static void deleteTable(OdpsConf odpsConf, String project, String table, boolean ifExists)
		throws RuntimeException {
		Odps odps = initOdps(odpsConf);

		try {
			odps.tables().delete(project, table, ifExists);
		} catch (OdpsException var6) {
			throw new RuntimeException(var6);
		}
	}

	public static DownloadSession getTableTunnelDownloadSession(OdpsConf odpsConf, String tableName, String partition)
		throws TunnelException, IOException {

		Account account = new AliyunAccount(odpsConf.getAccessId(), odpsConf.getAccessKey());
		Odps odps = new Odps(account);
		odps.setEndpoint(odpsConf.getEndpoint());
		odps.setDefaultProject(odpsConf.getProject());
		TableTunnel tunnel = new TableTunnel(odps);
		if (!odpsConf.getTunnelEndpoint().isEmpty()) {
			tunnel.setEndpoint(odpsConf.getTunnelEndpoint());
		}

		OdpsTableSchema tableSchema = getODPSTableSchema(odps, tableName);
		DownloadSession downloadSession;
		if (tableSchema.isPartition() && null != partition && !partition.isEmpty()) {
			PartitionSpec partitionSpec = new PartitionSpec(partition);
			downloadSession = tunnel.createDownloadSession(odpsConf.getProject(), tableName, partitionSpec);
		} else {
			downloadSession = tunnel.createDownloadSession(odpsConf.getProject(), tableName);
		}

		return downloadSession;
	}
}
