package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Comparator;
import java.util.List;

/**
 * {@link TFTableModelPredictFlatModelMapper} provides inference for SavedModel which is in Alink Model format.
 */
public class TFTableModelPredictFlatModelMapper extends FlatModelMapper implements Serializable {

	private final BaseTFSavedModelPredictRowFlatMapper mapper;

	public TFTableModelPredictFlatModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		mapper = new BaseTFSavedModelPredictRowFlatMapper(dataSchema, params);
	}

	@Override
	public TableSchema getOutputSchema() {
		return mapper.getOutputSchema();
	}

	@Override
	public void open() {
		mapper.open();
	}

	@Override
	public void close() {
		mapper.close();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		String workDir;
		try {
			workDir = PythonFileUtils.createTempWorkDir("temp_");
		} catch (Exception e) {
			throw new RuntimeException("Cannot create temporary work directory.", e);
		}

		modelRows = new ArrayList <>(modelRows);
		modelRows.sort(Comparator.comparingLong(d -> (Long) d.getField(0)));

		String zipFilename = (String) modelRows.get(0).getField(1);
		Path zipPath = Paths.get(workDir, zipFilename);
		try (FileOutputStream fos = new FileOutputStream(zipPath.toFile())) {
			Decoder decoder = Base64.getDecoder();
			for (int i = 1, modelRowsSize = modelRows.size(); i < modelRowsSize; i += 1) {
				Row modelRow = modelRows.get(i);
				fos.write(decoder.decode((String) modelRow.getField(1)));
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("Cannot extract data to %s", zipFilename), e);
		}

		String targetDir = zipFilename.substring(0, zipFilename.indexOf(".zip"));
		Path targetPath = Paths.get(workDir, targetDir);
		try {
			ZipFileUtil.unzipFileIntoDirectory(zipPath.toFile(), targetPath.toFile());
		} catch (IOException e) {
			throw new RuntimeException(String.format("Failed to unzip %s to %s.", zipPath.toString(), targetPath.toString()), e);
		}
		mapper.setModelPath(targetPath.toAbsolutePath().toString());
	}

	@Override
	public void flatMap(Row row, Collector <Row> output) throws Exception {
		mapper.flatMap(row, output);
	}
}
