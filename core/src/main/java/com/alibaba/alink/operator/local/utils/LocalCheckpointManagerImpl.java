package com.alibaba.alink.operator.local.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.local.sink.AkSinkLocalOp;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class LocalCheckpointManagerImpl implements LocalCheckpointManagerInterface {

	final static String SAVED_NODES_FILE = "savedNodes.txt";
	final String rootPath;

	List <String> savedNodeNames;
	Set <String> registeredNodeNames;

	public LocalCheckpointManagerImpl(String rootPath) {
		this.rootPath = rootPath + (rootPath.endsWith(File.separator) ? "" : File.separator);
		this.savedNodeNames = getSavedNodesFromFile();
		this.registeredNodeNames = new TreeSet <>();
	}

	private List <String> getSavedNodesFromFile() {
		List <String> result = new ArrayList <>();
		try {
			File file = new File(this.rootPath + SAVED_NODES_FILE);
			if (file.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					result.add(line);
				}
				reader.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error in reading file " + SAVED_NODES_FILE);
		}
		return result;
	}

	private void appendSavedNodeToFile(String nodeName) {
		try {
			File file = new File(this.rootPath + SAVED_NODES_FILE);
			FileWriter writer = new FileWriter(file, true);
			writer.write(nodeName + "\n");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error in updating file " + SAVED_NODES_FILE);
		}
	}

	void updateSavedNodesFile() {
		try {
			File file = new File(this.rootPath + SAVED_NODES_FILE);
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));
			for (String nodeName : this.savedNodeNames) {
				writer.write(nodeName + "\n");
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error in updating file " + SAVED_NODES_FILE);
		}
	}

	@Override
	public synchronized void registerNode(String nodeName) {
		this.registeredNodeNames.add(nodeName);

		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			printStatus();
		}
	}

	@Override
	public boolean isRegisteredNode(String nodeName) {
		return this.registeredNodeNames.contains(nodeName);
	}

	@Override
	public synchronized void setNodeSaved(String nodeName) {
		appendSavedNodeToFile(nodeName);
		this.savedNodeNames.add(nodeName);

		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			printStatus();
		}
	}

	@Override
	public boolean isSavedNode(String nodeName) {
		return savedNodeNames.contains(nodeName);
	}

	@Override
	public void removeUnfinishedNodeDir(String nodeName) {
		if (!isSavedNode(nodeName)) {
			deleteAll(new File(this.rootPath + nodeName));
		}
	}

	public void clearNodesAfter(String nodeName) {
		int m = this.savedNodeNames.indexOf(nodeName);
		if (m >= 0) {
			this.savedNodeNames = this.savedNodeNames.subList(0, m + 1);
			updateSavedNodesFile();
			clearUnsavedNodes();
		}
	}

	public void clearUnsavedNodes() {
		File rootDir = new File(this.rootPath);
		Set <String> deleteFiles = new TreeSet <>(Arrays.asList(rootDir.list()));
		deleteFiles.remove(SAVED_NODES_FILE);
		deleteFiles.removeAll(this.savedNodeNames);

		for (String fileName : deleteFiles) {
			deleteAll(new File(this.rootPath + fileName));
		}
	}

	@Override
	public synchronized void saveNodeOutput(String nodeName, MTable output, boolean overwrite) {
		File nodeDir = new File(this.rootPath + nodeName);
		nodeDir.mkdirs();

		new MemSourceLocalOp(output).link(
			new AkSinkLocalOp()
				.setFilePath(nodeDir.getAbsolutePath() + File.separator + "output.ak")
				.setOverwriteSink(overwrite)
		);
	}

	@Override
	public synchronized void saveNodeSideOutputs(String nodeName, MTable[] sideOutputs, boolean overwrite) {
		File nodeDir = new File(this.rootPath + nodeName);
		nodeDir.mkdirs();

		for (int i = 0; i < sideOutputs.length; i++) {
			String filePath = nodeDir.getAbsolutePath() + File.separator + "side_output_" + i + ".ak";
			File file = new File(filePath);

			if (file.exists() && !overwrite) {
				throw new AkIllegalDataException("file(" + filePath + ") already exists.");
			}

			if (null != sideOutputs[i]) {
				new MemSourceLocalOp(sideOutputs[i])
					.link(
						new AkSinkLocalOp()
							.setFilePath(filePath)
							.setOverwriteSink(overwrite)
					);
			} else {
				if (file.exists()) {
					file.delete();
				}
				try {
					file.createNewFile();
				} catch (Exception ex) {
					throw new RuntimeException("Fail to create file: " + filePath);
				}
			}
		}
	}

	@Override
	public synchronized MTable loadNodeOutput(String nodeName) {
		try {
			File nodeDir = new File(this.rootPath + nodeName);
			Tuple2 <TableSchema, List <Row>> content =
				AkUtils.readFromPath(new FilePath(nodeDir.getAbsolutePath() + File.separator + "output.ak"));

			return new MTable(content.f1, content.f0);
		} catch (Exception ex) {
			throw new AkIllegalDataException("Error in loading " + nodeName);
		}
	}

	@Override
	public synchronized int countNodeSideOutputs(String nodeName) {
		File nodeDir = new File(this.rootPath + nodeName);
		if (nodeDir.exists()) {
			return nodeDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith("side_output_");
				}
			}).length;
		} else {
			return 0;
		}
	}

	@Override
	public synchronized MTable loadNodeSideOutput(String nodeName, int index) {
		File nodeDir = new File(this.rootPath + nodeName);
		String filePath = nodeDir.getAbsolutePath() + File.separator + "side_output_" + index + ".ak";
		File file = new File(filePath);
		if (file.exists() && file.length() > 0) {
			try {
				Tuple2 <TableSchema, List <Row>> content = AkUtils.readFromPath(new FilePath(filePath));
				return new MTable(content.f1, content.f0);
			} catch (Exception ex) {
				throw new AkIllegalDataException("Error in loading " + nodeName);
			}
		} else {
			return null;
		}
	}

	private static void deleteAll(File folder) {
		if (folder.isDirectory()) {
			File[] files = folder.listFiles();
			if (files != null) {
				for (File file : files) {
					deleteAll(file);
				}
			}
		}
		folder.delete();
	}

	@Override
	public void printStatus() {
		System.out.println("Checkpoint : " + rootPath);
		System.out.println("Registered nodes : " + Arrays.toString(this.registeredNodeNames.toArray()));
		System.out.println("Saved nodes : " + Arrays.toString(this.savedNodeNames.toArray()));
		System.out.println();
	}
}
