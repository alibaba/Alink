package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.operator.common.dataproc.FFT;
import com.alibaba.alink.params.feature.DCTParams;
import org.apache.commons.math3.complex.Complex;

/**
 * Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 * with same length in the frequency domain.
 */
public class DCTMapper extends SISOMapper {
	private boolean inverse;

	public DCTMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		inverse = params.get(DCTParams.INVERSE);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return Types.STRING;
	}

	@Override
	protected Object mapColumn(Object in) throws Exception {
		if (null == in) {
			return null;
		}
		DenseVector input = VectorUtil.getDenseVector(in);
		if (inverse == false) {
			//forward DCT

			//transform input
			int length = input.size();
			Complex[] newInput = new Complex[length];
			for (int index = 0; index < (length + 1) / 2; index++) {
				newInput[index] = new Complex(input.get(2 * index));
			}
			for (int index = (length + 1) / 2; index < length; index++) {
				newInput[index] = new Complex(input.get(2 * length - 2 * index - 1));
			}

			//perform fft
			Complex[] fftResult;
			int logl = (int) (Math.log(length + 0.01) / Math.log(2));
			int nextLength = 1 << (logl + 2);

			if ((1 << logl) == length) {
				Complex[] omega = FFT.getOmega(length);
				fftResult = FFT.fftRadix2CooleyTukey(newInput, false, omega);
			} else {
				Complex[] omega = FFT.getOmega(nextLength);
				Complex[] omega2 = FFT.getOmega(length * 2);
				fftResult = FFT.fftChirpZ(newInput, false, omega, omega2);
			}

			//transform output, take real part, and output
			//notice: our DCT has a normalization factor (sqrt(2/length))
			//as well as additional sqrt(2) on [0]
			Complex unit4 = new Complex(Math.cos(Math.PI / length / 2.0),
				Math.sin(Math.PI / length / 2.0));
			Complex currentUnit = new Complex(1, 0);

			DenseVector output = new DenseVector(length);
			double norm_factor = Math.sqrt(2.0 / length);
			for (int index = 0; index < length; index++) {
				double currentReal = (fftResult[index].multiply(currentUnit.conjugate())).getReal() * norm_factor;
				if (index == 0) {
					currentReal /= Math.sqrt(2.0);
				}
				output.set(index, currentReal);
				currentUnit = currentUnit.multiply(unit4);
			}
			return VectorUtil.toString(output);
		} else {
			//inverse DCT

			//transform input
			int length = input.size();

			Complex unit4 = new Complex(Math.cos(Math.PI / length / 2.0),
				Math.sin(Math.PI / length / 2.0));
			Complex currentUnit = new Complex(1, 0);

			Complex[] newInput = new Complex[length];
			for (int index = 0; index < length; index++) {
				if (index == 0) {
					newInput[index] = new Complex(input.get(index));
				} else {
					newInput[index] = new Complex(input.get(index), -input.get(length - index));
				}
				newInput[index] = newInput[index].multiply(currentUnit).multiply(Math.sqrt(length));
				if (index > 0) {
					newInput[index] = newInput[index].divide(Math.sqrt(2.0));
				}
				currentUnit = currentUnit.multiply(unit4);
			}

			//perform ifft
			Complex[] fftResult;
			int logl = (int) (Math.log(length + 0.01) / Math.log(2));
			int nextLength = 1 << (logl + 2);

			if ((1 << logl) == length) {
				Complex[] omega = FFT.getOmega(length);
				fftResult = FFT.fftRadix2CooleyTukey(newInput, true, omega);
			} else {
				Complex[] omega = FFT.getOmega(nextLength);
				Complex[] omega2 = FFT.getOmega(length * 2);
				fftResult = FFT.fftChirpZ(newInput, true, omega, omega2);
			}

			//get result
			DenseVector output = new DenseVector(length);
			for (int index = 0; index < length; index++) {
				double currentReal = fftResult[index].getReal();
				if (index < (length + 1) / 2) {
					output.set(index * 2, currentReal);
				} else {
					output.set(length * 2 - index * 2 - 1, currentReal);
				}
			}
			return VectorUtil.toString(output);
		}
	}

}
