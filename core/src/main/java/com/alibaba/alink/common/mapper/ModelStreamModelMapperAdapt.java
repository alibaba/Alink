package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils.ModelStreamFileSource;
import com.alibaba.alink.params.mapper.ModelMapperParams;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ModelStreamModelMapperAdapt extends Mapper {
	private final FilePath modelPath;
	private final long scanInterval;
	private final Timestamp startTime;

	private final ModelMapper initialModelMapper;

	private final transient AtomicReference <ModelMapper> internal = new AtomicReference <>();

	private transient Thread modelMapperLoaderThread;
	private transient ModelStreamFileSource source;
	private transient ModelMapperLoader modelMapperLoader;

	public ModelStreamModelMapperAdapt(ModelMapper initialModelMapper) {

		super(initialModelMapper.getDataSchema(), getParamsFromModelMapper(initialModelMapper));

		this.ioSchema = initialModelMapper.ioSchema;

		checkIoSchema();

		initializeSliced();

		if (!ModelStreamUtils.useModelStreamFile(params)) {
			throw new IllegalArgumentException("Should be set the file path of model stream.");
		}

		this.modelPath = FilePath.deserialize(params.get(ModelMapperParams.MODEL_STREAM_FILE_PATH));
		this.scanInterval = ModelStreamUtils.createScanIntervalMillis(
			params.get(ModelMapperParams.MODEL_STREAM_SCAN_INTERVAL));
		this.startTime = ModelStreamUtils.createStartTime(params.get(ModelMapperParams.MODEL_STREAM_START_TIME));

		this.initialModelMapper = initialModelMapper;
	}

	private static Params getParamsFromModelMapper(ModelMapper modelMapper) {
		return modelMapper.params.clone();
	}

	public static boolean useModelStreamFile(ModelMapper modelMapper) {
		return ModelStreamUtils.useModelStreamFile(getParamsFromModelMapper(modelMapper));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		internal.get().map(selection, result);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		return null;
	}

	@Override
	public void open() {
		internal.set(initialModelMapper);

		source = new ModelStreamFileSource(modelPath, scanInterval, startTime);
		source.open();

		modelMapperLoader = new ModelMapperLoader();

		modelMapperLoaderThread = new Thread(() -> modelMapperLoader.run());

		modelMapperLoaderThread.start();
	}

	@Override
	public void close() {
		if (modelMapperLoader != null) {
			modelMapperLoader.cancel();
		}

		try {
			modelMapperLoaderThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		if (source != null) {
			try {
				source.close();
			} catch (IOException ex) {
				// pass
			}
		}
	}

	private class ModelMapperLoader {

		private final transient List <Row> cache = new ArrayList <>();

		private volatile boolean isRunning;

		public void run() {
			isRunning = true;
			Iterator <Row> modelIterator = source.iterator();

			while (isRunning && modelIterator.hasNext()) {
				Row rowWithId = modelIterator.next();

				long count = ModelStreamUtils.getCountFromRowInternal(rowWithId);

				Row modelRow = ModelStreamUtils.genRowWithoutIdentifierInternal(rowWithId);

				cache.add(modelRow);

				if (cache.size() == count) {
					// model load complete
					ModelMapper modelMapper;

					try {

						modelMapper = initialModelMapper.createNew(cache);

						modelMapper.open();

					} catch (Exception exception) {
						// pass

						cache.clear();
						continue;
					}

					cache.clear();

					internal.set(modelMapper);
				}
			}
		}

		public void cancel() {
			this.isRunning = false;
		}
	}
}
