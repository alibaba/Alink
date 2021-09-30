package com.alibaba.alink.operator.common.image;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.image.WriteTensorToImageParams;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;

public class WriteTensorToImageMapper extends Mapper {

	private final FilePath rootPath;
	private final String imageType;

	public WriteTensorToImageMapper(
		TableSchema dataSchema, Params params) {

		super(dataSchema, params);

		rootPath = FilePath.deserialize(params.get(WriteTensorToImageParams.ROOT_FILE_PATH));
		imageType = params.get(WriteTensorToImageParams.IMAGE_TYPE).toString();
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		FloatTensorToImage.write(
			FloatTensor.of(TensorUtil.getTensor(selection.get(0))),
			new FilePath(new Path(rootPath.getPath(), (String) selection.get(1)), rootPath.getFileSystem()),
			imageType
		);

		result.set(0, selection.get(0));
		result.set(1, selection.get(1));
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		String tensorCol = params.get(WriteTensorToImageParams.TENSOR_COL);
		String relativeFilePathCol = params.get(WriteTensorToImageParams.RELATIVE_FILE_PATH_COL);

		return Tuple4.of(
			new String[] {tensorCol, relativeFilePathCol},
			new String[] {tensorCol, relativeFilePathCol},
			new TypeInformation <?>[] {
				TableUtil.findColTypeWithAssertAndHint(dataSchema, tensorCol),
				TableUtil.findColTypeWithAssertAndHint(dataSchema, relativeFilePathCol)
			},
			params.get(WriteTensorToImageParams.RESERVED_COLS)
		);
	}

	public static class FloatTensorToImage {

		private static String getFormat(FilePath filePath, String defaultFormatName) {
			String fileName = filePath.getPath().getName();
			String format = defaultFormatName;

			int index = fileName.lastIndexOf(".");

			// skip that file starts with dot.
			if (index > 0) {
				format = fileName.substring(index + 1);
			}

			if (ImageIO.getImageWritersBySuffix(format).hasNext()) {
				return format;
			} else {
				throw new IllegalArgumentException(String.format("Could not write the image with suffix: %s", format));
			}
		}

		public static void write(
			FloatTensor floatTensor, FilePath filePath, String defaultFormatName) throws IOException {

			FloatTensorToImage.writeToFile(
				FloatTensorToImage.writeToImage(floatTensor), filePath, defaultFormatName
			);
		}

		public static void writeToFile(
			BufferedImage bufferedImage, FilePath filePath, String defaultFormatName) throws IOException {

			filePath.getFileSystem().mkdirs(filePath.getPath().getParent());

			try (ImageOutputStream outputStream = ImageIO.createImageOutputStream(
				filePath.getFileSystem().create(filePath.getPath(), WriteMode.OVERWRITE))) {

				ImageIO.write(bufferedImage, getFormat(filePath, defaultFormatName), outputStream);
			}
		}

		public static BufferedImage writeToImage(FloatTensor floatTensor) {
			long[] shape = floatTensor.shape();

			Preconditions.checkArgument(shape != null && shape.length == 3);

			int numBands = (int) shape[2];
			int width = (int) shape[1];
			int height = (int) shape[0];

			int imageType;

			if (numBands == 3) {
				imageType = BufferedImage.TYPE_INT_RGB;
			} else if (numBands == 4) {
				imageType = BufferedImage.TYPE_INT_ARGB;
			} else {
				throw new IllegalArgumentException(
					String.format("Unsupported tensor to image. num bands: %d", numBands));
			}

			// rollback from normalized to rgb
			floatTensor.scale(255.0f);

			BufferedImage bufferedImage = new BufferedImage(width, height, imageType);

			WritableRaster raster = bufferedImage.getRaster();

			int minX = raster.getMinX();
			int minY = raster.getMinY();

			for (int i = 0; i < shape[0]; ++i) {
				for (int j = 0; j < shape[1]; ++j) {
					for (int z = 0; z < shape[2]; ++z) {
						raster.setSample(minX + j, minY + i, z, floatTensor.getFloat(i, j, z));
					}
				}
			}

			return bufferedImage;
		}
	}
}
