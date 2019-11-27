package com.alibaba.alink.common.comqueue;

import org.apache.flink.api.java.DataSet;

/**
 * An BaseComQueue item for communication.
 */
public abstract class CommunicateFunction implements ComQueueItem {

    /**
     * Perform communication work.
     *
     * @param input     output of previous queue item.
     * @param sessionId session id for shared objects.
     * @param <T>       Type of dataset.
     * @return result dataset.
     */
	public abstract <T> DataSet <T> communicateWith(DataSet <T> input, int sessionId);

}
