package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.common.modelstream.ModelStreamFileScanner;
import com.alibaba.alink.operator.common.modelstream.ModelStreamFileScanner.ScanTask;
import com.alibaba.alink.operator.common.modelstream.ModelStreamUtils;
import com.alibaba.alink.params.ModelStreamScanParams;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ModelStreamModelMapperAdapter extends Mapper {
	private final FilePath modelPath;
	private final long scanInterval;
	private final Timestamp startTime;

	private final ModelMapper initialModelMapper;

	private final transient AtomicReference <ModelMapper> internal = new AtomicReference <>();

	private transient ModelStreamFileScanner fileScanner;

	public ModelStreamModelMapperAdapter(ModelMapper initialModelMapper) {

		super(initialModelMapper.getDataSchema(), getParamsFromModelMapper(initialModelMapper));

		this.ioSchema = initialModelMapper.ioSchema;

		checkIoSchema();

		initializeSliced();

		if (!ModelStreamUtils.useModelStreamFile(params)) {
			throw new AkIllegalOperatorParameterException("Should be set the file path of model stream.");
		}

		this.modelPath = FilePath.deserialize(params.get(ModelStreamScanParams.MODEL_STREAM_FILE_PATH));
		this.scanInterval = ModelStreamUtils.createScanIntervalMillis(
			params.get(ModelStreamScanParams.MODEL_STREAM_SCAN_INTERVAL)
		);
		this.startTime = ModelStreamUtils.createStartTime(
			params.get(ModelStreamScanParams.MODEL_STREAM_START_TIME)
		);

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
	public Row map(Row row) throws Exception {
		return internal.get().map(row);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		return null;
	}

	@Override
	public void open() {
		initialModelMapper.open();
		internal.set(initialModelMapper);
		fileScanner = new ModelStreamFileScanner(1, 2);
		fileScanner.open();

		fileScanner.scanToUpdateModel(
			new ScanTask(modelPath, startTime),
			Time.of(scanInterval, TimeUnit.MILLISECONDS),
			initialModelMapper::createNew,
			internal
		);
	}

	@Override
	public void close() {
		if (fileScanner != null) {
			fileScanner.close();
		}
	}
}
