/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.util.InstantiationUtil.serializeObject;

/**
 * SourceUtils.
 */
public class SourceUtils {

	/**
	 * Mod assign.
	 *
	 * @param sourceName    the source name
	 * @param consumerCount the consumer count
	 * @param consumerIndex the consumer index
	 * @param partitions    the partitions
	 * @return the list
	 */
	public static List<Integer> modAssign(
			String sourceName,
			int consumerCount,
			int consumerIndex,
			int partitions) {
		argumentsCheck(sourceName, consumerCount, consumerIndex, partitions);

		List<Integer> assignedPartitions = new LinkedList<>();
		for (int i = 0; i < partitions; i++) {
			if (i % consumerCount == consumerIndex) {
				assignedPartitions.add(i);
			}
		}
		return assignedPartitions;
	}

	/**
	 * Division assign list.
	 *
	 * @param sourceName    the source name
	 * @param consumerCount the consumer count
	 * @param consumerIndex the consumer index
	 * @param partitions    the partitions
	 * @return the list
	 */
	public static List<Integer> divisionAssign(
			String sourceName,
			int consumerCount,
			int consumerIndex,
			int partitions) {
		argumentsCheck(sourceName, consumerCount, consumerIndex, partitions);
		Preconditions.checkState(partitions % consumerCount == 0);

		List<Integer> assignedPartitions = new LinkedList<>();

		int assignedPartitionCount = partitions / consumerCount;
		int startIndex = assignedPartitionCount * consumerIndex;
		for (int i = 0; i < assignedPartitionCount; i++) {
			assignedPartitions.add(startIndex + i);
		}
		return assignedPartitions;
	}

	/**
	 * Division mod assign list.
	 *
	 * @param sourceName    the source name
	 * @param consumerCount the consumer count
	 * @param consumerIndex the consumer index
	 * @param partitions    the partitions
	 * @return the list
	 */
	public static List<Integer> divisionModAssign(
			String sourceName,
			int consumerCount,
			int consumerIndex,
			int partitions) {
		argumentsCheck(sourceName, consumerCount, consumerIndex, partitions);

		List<Integer> assignedPartitions = new LinkedList<>();

		int assignedPartitionCount = partitions / consumerCount;
		int startIndex = assignedPartitionCount * consumerIndex;
		for (int i = 0; i < assignedPartitionCount; i++) {
			assignedPartitions.add(startIndex + i);
		}
		int modAssigned = assignedPartitionCount * consumerCount + consumerIndex;
		if (partitions > modAssigned) {
			assignedPartitions.add(modAssigned);
		}
		return assignedPartitions;
	}

	/**
	 * Range[startIndex, count] assigned.
	 * The last range may be larger than the others.
	 *
	 * @param sourceName       the source name
	 * @param consumerCount    the consumer count
	 * @param consumerIndex    the consumer index
	 * @param totalRecordCount the total record count
	 * @return the start index and count
	 */
	public static Tuple2<Long, Long> rangeAssign(
			String sourceName,
			int consumerCount,
			int consumerIndex,
			long totalRecordCount) {
		argumentsCheck(sourceName, consumerCount, consumerIndex);

		if (totalRecordCount <= consumerIndex) {
			// Do not assign for this consumer.
			return null;
		}

		long assignedPartitionCount = totalRecordCount / consumerCount;
		if (assignedPartitionCount == 0) {
			return new Tuple2<>((long) consumerIndex, 1L);
		}
		long startIndex = assignedPartitionCount * consumerIndex;
		long count = consumerIndex + 1 == consumerCount ? totalRecordCount - startIndex : assignedPartitionCount;
		return new Tuple2<>(startIndex, count);
	}

	/**
	 * Parse date string long.
	 *
	 * @param formatString the format string
	 * @param dateString   the date string
	 * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * @throws ParseException the parse exception
	 */
	public static Long parseDateString(
			String formatString,
			String dateString) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatString);
		return simpleDateFormat.parse(dateString).getTime();
	}

	public static String toDateString(
			String formatString,
			Long ts) {
		Timestamp timestamp = new Timestamp(ts);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatString);
		return simpleDateFormat.format(timestamp);

	}

	/**
	 * Arguments check.
	 *
	 * @param sourceName    the source name
	 * @param consumerCount the consumer count
	 * @param consumerIndex the consumer index
	 * @param partitions    the partitions
	 */
	protected static void argumentsCheck(
			String sourceName,
			int consumerCount,
			int consumerIndex,
			int partitions) {
		argumentsCheck(sourceName, consumerCount, consumerIndex);

		Preconditions.checkArgument(partitions > 0,
				"Source: [" + sourceName + "], partition count: " + partitions + " must be more than 0.");
		Preconditions.checkState(partitions >= consumerCount,
				"Source: [" + sourceName + "], partition count: " + partitions + " is less than consumer count: " +
						consumerCount + "\n 源表: [" + sourceName + "] 的分区数目 " + partitions + " 小于资源配置文件中的 " +
						"Data Source并发度:" + consumerCount + "。请调整资源配置文件中Data Source的并发度不大于" + partitions);
	}

	/**
	 * Arguments check.
	 *
	 * @param sourceName    the source name
	 * @param consumerCount the consumer count
	 * @param consumerIndex the consumer index
	 */
	protected static void argumentsCheck(
			String sourceName,
			int consumerCount,
			int consumerIndex) {
		Preconditions.checkArgument(consumerCount > 0,
				"Source: [" + sourceName + "], Consumer count: " + consumerCount + " must be more than 0.");
		Preconditions.checkElementIndex(consumerIndex, consumerCount,
				"Source: [" + sourceName + "], Consumer index: " + consumerIndex + " is out of range of consumer count: " + consumerCount);
	}

	public static <T> T deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
		T ret = (T) ois.readObject();
		ois.close();
		return ret;
	}

	public static <T> T cloneObject(T src) throws IOException, ClassNotFoundException {
		return deserializeObject(serializeObject(src));
	}
}
