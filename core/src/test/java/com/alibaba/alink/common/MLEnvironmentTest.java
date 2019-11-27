package com.alibaba.alink.common;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for MLEnvironment.
 */
public class MLEnvironmentTest {
	@Test
	public void testDefaultConstructor() {
		MLEnvironment mlEnvironment = new MLEnvironment();
		Assert.assertNotNull(mlEnvironment.getExecutionEnvironment());
		Assert.assertNotNull(mlEnvironment.getBatchTableEnvironment());
		Assert.assertNotNull(mlEnvironment.getStreamExecutionEnvironment());
		Assert.assertNotNull(mlEnvironment.getStreamTableEnvironment());
	}

	@Test
	public void testConstructWithBatchEnv() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(executionEnvironment);

		MLEnvironment mlEnvironment = new MLEnvironment(executionEnvironment, batchTableEnvironment);

		Assert.assertSame(mlEnvironment.getExecutionEnvironment(), executionEnvironment);
		Assert.assertSame(mlEnvironment.getBatchTableEnvironment(), batchTableEnvironment);
	}

	@Test
	public void testConstructWithStreamEnv() {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

		MLEnvironment mlEnvironment = new MLEnvironment(streamExecutionEnvironment, streamTableEnvironment);

		Assert.assertSame(mlEnvironment.getStreamExecutionEnvironment(), streamExecutionEnvironment);
		Assert.assertSame(mlEnvironment.getStreamTableEnvironment(), streamTableEnvironment);
	}
}