package com.alibaba.alink.operator.common.audio;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.operator.common.dataproc.MFCC;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;

public class ExtractMfccFeatureMapper extends SISOMapper {

	private final MFCC mfcc;

	public ExtractMfccFeatureMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		int sr = params.get(ExtractMfccFeatureParams.SAMPLE_RATE);
		int window = (int) (sr * params.get(ExtractMfccFeatureParams.WINDOW_TIME));
		int step = (int) (sr * params.get(ExtractMfccFeatureParams.HOP_TIME));
		int n_mfcc = 1 + params.get(ExtractMfccFeatureParams.NUM_MFCC);
		this.mfcc = new MFCC(sr, window, step, n_mfcc);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return TensorTypes.FLOAT_TENSOR;
	}

	@Override
	protected Object mapColumn(Object input) throws Exception {

		if (input instanceof FloatTensor) {
			FloatTensor ft = (FloatTensor) input;
			long[] shape = ft.shape();
			if (shape.length == 2) {
				// input shape is [channelNum,audioLength]
				// return shape is [channelNum, audioLength/frameStep, n_mfcc]
				FloatTensor[] tensors = Tensor.unstack(ft, 0, null);
				int channel_num = (int) shape[0];
				int audio_len = (int) tensors[0].size();
				int mfcc_len = (int) Math.ceil((audio_len - mfcc.frameLength + mfcc.frameStep) / mfcc.frameStep);
				int n_mfcc = mfcc.getnFeature();
				float[][][] tensorData = new float[mfcc_len][n_mfcc][channel_num];
				for (int c = 0; c < channel_num; c++) {
					double[] data = new double[audio_len];
					for (int k = 0; k < audio_len; k++) {
						data[k] = tensors[c].getFloat(k);
					}
					float[][] result = mfcc.process(data);
					for (int i = 0; i < mfcc_len; i++) {
						for (int j = 0; j < n_mfcc; j++) {
							tensorData[i][j][c] = result[i][j];
						}
					}
				}
				return new FloatTensor(tensorData);
			} else if (shape.length == 1) {
				// input shape is [audioLength]
				// return shape is [audioLength/frameStep, n_mfcc]
				int audio_len = (int) ft.size();
				double[] data = new double[audio_len];
				for (int k = 0; k < audio_len; k++) {
					data[k] = ft.getFloat(k);
				}
				return new FloatTensor(mfcc.process(data));
			} else {
				return null;
			}
		} else if (input instanceof Vector) {
			double[] vec = ((DenseVector) input).getData();
			return new FloatTensor(this.mfcc.process(vec));
		} else {
			return null;
		}

		//DoubleTensor tensor = (DoubleTensor) input;
		////DoubleTensor[] tensors = Tensor.unstack(tensor, 0, null);
		//
		//double[] vec = tensor.toVector().getData();
		//DenseVector sum = new DenseVector(mfcc.getnFeature());
		//double[][] mfccResult = this.mfcc.process(vec);
		//for (double[] d : mfccResult) {
		//	sum.plusEqual(new DenseVector(d));
		//}
		//sum.scaleEqual(1.0 / mfccResult.length);
		//
		////
		////		int n = (vec.length + 511) / 512;
		////
		////		DenseVector sum = new DenseVector(26);
		////		double[] frame = new double[512];
		////		int offset = (n - 1) * 512;
		////		for (int i = offset; i < vec.length; i++) {
		////			frame[i - offset] = vec[i];
		////		}
		////		//sum.plusEqual(new DenseVector(this.mfcc.getParameters(frame)));
		////		sum.plusEqual(new DenseVector(26));
		////
		////		for (int k = n - 2; k >= 0; k--) {
		////			offset = k * 512;
		////			frame = Arrays.copyOfRange(vec, offset, offset + 512);
		////			//sum.plusEqual(new DenseVector(this.mfcc.getParameters(frame)));
		////			sum.plusEqual(new DenseVector(26));
		////		}
		////
		////		sum.scaleEqual(1.0 / n);
		//
		//return sum;
	}
}
