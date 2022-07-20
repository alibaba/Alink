package com.alibaba.alink.operator.common.audio;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.common.dataproc.FFT;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;
import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import com.sun.media.sound.WaveFileReader;
import org.apache.commons.math3.complex.Complex;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Arrays;

public class ReadAudioToTensorMapper extends Mapper {
	private final FilePath rootFolder;
	private int sampleRate = 0;
	private double duration = -1;
	private double offset = 0;
	private final boolean channelFirst;

	public ReadAudioToTensorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		channelFirst = params.get(ReadAudioToTensorParams.CHANNEL_FIRST);
		rootFolder = FilePath.deserialize(params.get(ReadAudioToTensorParams.ROOT_FILE_PATH));
		sampleRate = params.get(ReadAudioToTensorParams.SAMPLE_RATE);
		duration = params.contains(ReadAudioToTensorParams.DURATION) ?
			params.get(ReadAudioToTensorParams.DURATION) : -1;
		offset = params.get(ReadAudioToTensorParams.OFFSET);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Tuple2 <Long, FloatTensor> res = AudioToFloatTensor.read(
			new FilePath(
				new Path(rootFolder.getPath(), (String) selection.get(0)),
				rootFolder.getFileSystem()
			)
			, sampleRate, duration, offset, channelFirst
		);
		result.set(0, res.f1);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		return Tuple4.of(
			new String[] {params.get(ReadAudioToTensorParams.RELATIVE_FILE_PATH_COL)},
			new String[] {params.get(ReadAudioToTensorParams.OUTPUT_COL)},
			new TypeInformation <?>[] {AlinkTypes.FLOAT_TENSOR},
			params.get(ReadAudioToTensorParams.RESERVED_COLS)
		);
	}

	public static final class AudioToFloatTensor {
		public static Tuple2 <Long, FloatTensor> read(FilePath filePath, int sampleRate, double duration,
													  double offset,
													  boolean channelFirst) {
			BaseFileSystem <?> fileSystem = filePath.getFileSystem();
			Path path = filePath.getPath();

			// 读取音频
			WaveFileReader waveFileReader = new WaveFileReader();

			AudioFileFormat wavFormat = null;
			try (InputStream inputStream = fileSystem.open(path)) {
				BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
				wavFormat = waveFileReader.getAudioFileFormat(bufferedInputStream);
			} catch (Exception e) {
				throw new AkIllegalDataException("cannot read the wav audio file " + path.getName());
			}
			// 获取音频基本数据
			AudioFormat format = wavFormat.getFormat();
			int frameLen = wavFormat.getFrameLength();
			int channelNum = format.getChannels();
			// 文件采样率和参数不一致时需要重采样
			float frameRate = format.getFrameRate();
			int frameSize = format.getFrameSize();
			int sampleSize = format.getSampleSizeInBits();// equal to frameSize*8
			// 计算采样起始位置
			sampleRate = sampleRate == -1 ? (int) frameRate : sampleRate;

			int sampleStart = (int) (offset * sampleRate);
			int sampleLen = duration == -1 ? (frameLen - sampleStart) : (int) (duration * sampleRate);
			int sampleEnd = Math.min(frameLen, sampleLen + sampleStart);

			// 拼读音频byte，转float
			float floatOffset;
			float floatScale;
			float[][] data = channelFirst || frameRate != sampleRate ? new float[channelNum][sampleLen]
				: new float[sampleLen][channelNum];
			if (sampleSize > 8) {
				// If more than 8 validBits, data is signed
				// Conversion required dividing by magnitude of max negative value
				floatOffset = 0;
				floatScale = 1 << (sampleSize - 1);
			} else {
				// Else if 8 or less validBits, data is unsigned
				// Conversion required dividing by max positive value
				floatOffset = -1;
				floatScale = (float) (0.5 * ((1 << sampleSize) - 1));
			}

			try (InputStream inputStream = fileSystem.open(path)) {
				AudioInputStream audioInputStream = waveFileReader.getAudioInputStream(inputStream);
				byte[] b = new byte[frameSize];
				for (int i = 0; i < frameLen; i++) {
					for (int j = 0; j < channelNum; j++) {
						audioInputStream.read(b);
						if (i >= sampleStart && i < sampleEnd) {
							if (channelFirst || frameRate != sampleRate) {
								data[j][i - sampleStart] = (floatOffset + byteToFloat(b) / floatScale);
							} else {
								data[i - sampleStart][j] = (floatOffset + byteToFloat(b) / floatScale);
							}
						}
					}
				}
			} catch (Exception e) {
				throw new AkIllegalDataException("cannot read the wav audio file " + path.getName());
			}

			frameLen = sampleEnd - sampleStart;
			if (frameRate != sampleRate) {
				// 重采样
				// 分为若干个长度512的窗口，计算重采样后长度，每个窗口用FFT插值算法
				int oldWindow = 512;
				int newWindow = (int) Math.floor(oldWindow * sampleRate / frameRate);
				int winNum = frameLen % oldWindow == 0 ? frameLen / oldWindow : frameLen / oldWindow + 1;
				float[][] resampleDate = channelFirst ? new float[channelNum][newWindow * winNum]
					: new float[newWindow * winNum][channelNum];
				float[] buf1 = new float[oldWindow];
				float[] buf2 = new float[newWindow];
				for (int i = 0; i < channelNum; i++) {
					for (int j = 0; j < winNum; j++) {
						int s1 = j * oldWindow;
						int t1 = (j + 1) * oldWindow;
						if (t1 > frameLen) {
							t1 = frameLen;
							// 末尾补0
							Arrays.fill(buf1, 0);
						}
						int s2 = j * newWindow;
						System.arraycopy(data[i], s1, buf1, 0, t1 - s1);
						buf2 = fftInterpolation(buf1, newWindow);
						for (int k = 0; k < newWindow; k++) {
							if (channelFirst) {
								resampleDate[i][s2 + k] = buf2[k];
							} else {
								resampleDate[s2 + k][i] = buf2[k];
							}
						}
					}
				}
				return Tuple2.of((long) sampleRate, new FloatTensor(resampleDate));

			}
			return Tuple2.of((long) frameRate, new FloatTensor(data));

		}

		static float byteToFloat(byte[] arr) {
			long value = 0;
			int bytesPerSample = arr.length;
			for (int i = 0; i < bytesPerSample; i++) {
				int v = arr[i];
				if (i < bytesPerSample - 1 || bytesPerSample == 1) {
					v &= 0xff;
				}
				value += v << (i * 8);
			}
			return (float) value;
		}

		static float[] fftInterpolation(float[] x, int n) {
			/**
			 * 计算原始序列的频域，假设有m个点，频域 K = [1...m]，插值结果记为py 有N个点
			 * 用傅立叶多项插值法，从频域逆变换出插值后的时域
			 * */
			int m = x.length;
			int k = (int) Math.floor((m + 1) / 2);
			float[] result = new float[n];

			Complex[] cpx = new Complex[m];
			for (int i = 0; i < m; i++) {
				cpx[i] = new Complex(x[i]);
			}
			Complex[] omega = FFT.getOmega(m);
			cpx = FFT.fftRadix2CooleyTukey(cpx, false, omega);

			for (int i = 0; i < n; i++) {
				double sum = cpx[0].getReal() / m;
				double cosBasic = 0;
				double cosLast = 0;
				double sinBasic = 0;
				double sinLast = 0;
				double cosCurr = 0;
				double sinCurr = 0;
				for (int j = 1; j <= k; j++) {
					if (j == 1) {
						cosBasic = Math.cos(2 * Math.PI * j * i / n);
						sinBasic = Math.sin(2 * Math.PI * j * i / n);
						cosCurr = cosBasic;
						sinCurr = sinBasic;
					} else {
						cosCurr = cosLast * cosBasic - sinLast * sinBasic;
						sinCurr = sinBasic * cosLast + sinLast * cosBasic;
					}
					sum = sum + 2 * cpx[j].getReal() / m * cosCurr - 2 * cpx[j].getImaginary() / m * sinCurr;
					cosLast = cosCurr;
					sinLast = sinCurr;
				}
				sum = sum + 2 * cpx[k + 1].getReal() / m * Math.cos(2 * Math.PI * i * (k + 1) / n);
				result[i] = (float) sum;
			}
			return result;
		}
	}
}
