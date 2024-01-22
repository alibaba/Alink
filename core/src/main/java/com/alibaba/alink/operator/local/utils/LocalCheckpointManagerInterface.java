package com.alibaba.alink.operator.local.utils;

import com.alibaba.alink.common.MTable;

public interface LocalCheckpointManagerInterface {

	void registerNode(String nodeName);

	boolean isRegisteredNode(String nodeName);

	void setNodeSaved(String nodeName);

	boolean isSavedNode(String nodeName);

	void removeUnfinishedNodeDir(String nodeName);

	void saveNodeOutput(String nodeName, MTable output, boolean overwrite);

	void saveNodeSideOutputs(String nodeName, MTable[] sideOutputs, boolean overwrite);

	MTable loadNodeOutput(String nodeName);

	int countNodeSideOutputs(String nodeName);

	MTable loadNodeSideOutput(String nodeName, int index);

	void printStatus();
}
