package com.alibaba.alink.operator.common.image;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.image.ReadImageToTensorParams;
import javax.imageio.ImageIO;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.awt.image.Raster;
import java.io.IOException;

public class ReadImageToTensorMapper extends Mapper {

	private final FilePath rootFolder;
	private final boolean resized;
	private final int newWidth;
	private final int newHeight;

	public ReadImageToTensorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		rootFolder = FilePath.deserialize(params.get(ReadImageToTensorParams.ROOT_FILE_PATH));
		if (params.contains(ReadImageToTensorParams.IMAGE_WIDTH)
			&& params.contains(ReadImageToTensorParams.IMAGE_HEIGHT)) {
			resized = true;
			newWidth = params.get(ReadImageToTensorParams.IMAGE_WIDTH);
			newHeight = params.get(ReadImageToTensorParams.IMAGE_HEIGHT);
		} else {
			resized = false;
			newWidth = -1;
			newHeight = -1;
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		result.set(
			0,
			ImageToFloatTensor
				.read(
					new FilePath(
						new Path(rootFolder.getPath(), (String) selection.get(0)),
						rootFolder.getFileSystem()
					),
					resized, newHeight, newWidth
				)
		);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {

		return Tuple4.of(
			new String[] {params.get(ReadImageToTensorParams.RELATIVE_FILE_PATH_COL)},
			new String[] {params.get(ReadImageToTensorParams.OUTPUT_COL)},
			new TypeInformation <?>[] {TensorTypes.FLOAT_TENSOR},
			params.get(ReadImageToTensorParams.RESERVED_COLS)
		);
	}

	/**
	 * To tensor of CHW format. Ref: https://github.com/Microsoft/CNTK/issues/276.
	 */
	public static final class ImageToFloatTensor {

		public static FloatTensor read(FilePath filePath, boolean resized, int newHeight, int newWidth)
			throws IOException {
			return readToTensor(readImageFromFile(filePath, resized, newHeight, newWidth));
		}

		private static BufferedImage readImageFromFile(FilePath filePath, boolean resized, int newHeight, int newWidth)
			throws IOException {

			BufferedImage bufferedImage = ImageIO.read(
				ImageIO.createImageInputStream(filePath.getFileSystem().open(filePath.getPath()))
			);

			if (resized) {
				BufferedImage outputImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB);

				outputImage.getGraphics().drawImage(
					bufferedImage.getScaledInstance(newWidth, newHeight, Image.SCALE_SMOOTH),
					0, 0, null
				);

				return outputImage;
			} else {
				return bufferedImage;
			}
		}

		public static BufferedImage toRGBImage(BufferedImage bufferedImage) {
			int imageType = bufferedImage.getType();
			int newImageType;

			switch (imageType) {
				case BufferedImage.TYPE_INT_ARGB:
				case BufferedImage.TYPE_INT_RGB:
					newImageType = imageType;
					break;
				case BufferedImage.TYPE_3BYTE_BGR:
					newImageType = BufferedImage.TYPE_INT_RGB;
					break;
				case BufferedImage.TYPE_4BYTE_ABGR:
					newImageType = BufferedImage.TYPE_INT_ARGB;
					break;
				default:
					throw new IllegalArgumentException(String.format("Unsupported imageType: %d", imageType));
			}

			if (imageType == newImageType) {
				return bufferedImage;
			} else {
				BufferedImage newBufferedImage = new BufferedImage(
					bufferedImage.getWidth(),
					bufferedImage.getHeight(),
					newImageType
				);

				return new ColorConvertOp(
					bufferedImage.getColorModel().getColorSpace(),
					newBufferedImage.getColorModel().getColorSpace(),
					null
				).filter(bufferedImage, newBufferedImage);
			}
		}

		private static FloatTensor readToTensor(BufferedImage bufferedImage) {
			bufferedImage = toRGBImage(bufferedImage);

			Raster raster = bufferedImage.getData();

			int numBands = raster.getNumBands();
			int height = raster.getHeight();
			int width = raster.getWidth();
			int minX = raster.getMinX();
			int minY = raster.getMinY();
			float[] sample = new float[numBands];

			float[][][] img = new float[height][width][numBands];

			for (int h = 0; h < height; ++h) {
				for (int w = 0; w < width; ++w) {
					raster.getPixel(minX + w, minY + h, sample);
					System.arraycopy(sample, 0, img[h][w], 0, numBands);
				}
			}

			// normalize to (0.0, 1.0)
			return new FloatTensor(img).scale(1f / 255.0f);
		}
	}
}
