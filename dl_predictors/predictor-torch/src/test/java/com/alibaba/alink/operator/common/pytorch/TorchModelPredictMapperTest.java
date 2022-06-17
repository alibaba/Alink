package com.alibaba.alink.operator.common.pytorch;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.TorchModelPredictParams;
import io.reactivex.rxjava3.functions.BiFunction;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.StdArrays;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TorchModelPredictMapperTest {
	@Test
	public void testMap() throws Exception {
		Row item = Row.of(new LongTensor(new long[][] {{1, 2, 3}, {4, 5, 6}}), new IntTensor(3));
		Params params = new Params();
		params.set(TorchModelPredictParams.MODEL_PATH, "res:///pytorch.pt");
		params.set(TorchModelPredictParams.SELECTED_COLS, new String[] {"f", "d"});
		params.set(TorchModelPredictParams.OUTPUT_SCHEMA_STR, "p LONG_TENSOR");
		params.set(TorchModelPredictParams.RESERVED_COLS, new String[] {});
		params.set(TorchModelPredictParams.INTRA_OP_PARALLELISM, 1);
		TableSchema dataSchema = TableUtil.schemaStr2Schema("f LONG_TENSOR, d LONG_TENSOR");
		TorchModelPredictMapper mapper = new TorchModelPredictMapper(dataSchema, params);
		mapper.open();
		for (int i = 0; i < 100; i += 1) {
			Row output = mapper.map(item);
			LongTensor t = (LongTensor) output.getField(0);
			Assert.assertArrayEquals(new long[] {11, 13, 15},
				StdArrays.array1dCopyOf((LongNdArray) TensorInternalUtils.getTensorData(t)));
		}
		mapper.close();
	}

	public static <T extends Mapper> void testMultiThreadMapper(BiFunction <TableSchema, Params, T> constructor,
																TableSchema dataSchema, Params params, Row[] inputs,
																int numThreads) throws Throwable {
		int numItems = inputs.length;
		T mapper = constructor.apply(dataSchema, params);
		mapper.open();
		Row[] outputs = new Row[numItems];
		for (int i = 0; i < numItems; i += 1) {
			outputs[i] = mapper.map(inputs[i]);
		}
		mapper.close();

		T multiThreadMapper = constructor.apply(dataSchema, params);
		multiThreadMapper.open();

		ExecutorService executorService = new ThreadPoolExecutor(
			numThreads, numThreads, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue <>(numThreads));

		Future <?>[] futures = new Future[numThreads];
		for (int threadId = 0; threadId < numThreads; threadId += 1) {
			futures[threadId] = executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						for (int i = 0; i < numItems; i += 1) {
							Row output = multiThreadMapper.map(inputs[i]);
							Assert.assertEquals(output.toString(), outputs[i].toString());
							Thread.sleep(10);
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		for (int i = 0; i < numThreads; i += 1) {
			futures[i].get();
		}
		multiThreadMapper.close();
	}

	@Test
	public void testMapMultiThread() throws Throwable {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Row item = Row.of(new LongTensor(new long[][] {{1, 2, 3}, {4, 5, 6}}), new IntTensor(3));
		int numItems = 100;
		Row[] rows = new Row[numItems];
		for (int i = 0; i < numItems; i += 1) {
			rows[i] = item;
		}

		Params params = new Params();
		params.set(TorchModelPredictParams.MODEL_PATH, "res:///pytorch.pt");
		params.set(TorchModelPredictParams.SELECTED_COLS, new String[] {"f", "d"});
		params.set(TorchModelPredictParams.OUTPUT_SCHEMA_STR, "p LONG_TENSOR");
		params.set(TorchModelPredictParams.RESERVED_COLS, new String[] {});
		params.set(TorchModelPredictParams.INTRA_OP_PARALLELISM, 1);
		TableSchema dataSchema = TableUtil.schemaStr2Schema("f LONG_TENSOR, d LONG_TENSOR");
		testMultiThreadMapper(TorchModelPredictMapper::new, dataSchema, params, rows, 2);
	}
}
