package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.operator.common.feature.DCTMapper;
import org.apache.commons.math3.complex.Complex;

import java.io.Serializable;
import java.util.Arrays;

public class MFCC implements Serializable {
	// filter to amplify the high freq, 0.97 or 0.95
	private static final double PRE_EMPHASISe = 0.97;
	// Hamming window parameter
	private static final double HAMMING = 0.46;
	private static final double PI = Math.PI;
	private float sampleRate;
	public int frameLength;
	public int frameStep;
	private int nFFT;
	private int nFilt;
	private int nFeature;
	private Complex[] omega;
	private double[][] filterBank;
	private double[] hamming_function;

	public int getnFeature() {
		return nFeature;
	}

	public MFCC(float sampleRate, int frameLength, int stepSize, int melFilterNum) {
		int outputFeatureNum = melFilterNum - 1;

		this.frameLength = frameLength;
		this.frameStep = stepSize;
		this.sampleRate = sampleRate;
		this.nFFT = frameLength;
		this.nFilt = melFilterNum;
		this.nFeature = outputFeatureNum;

		// step 1: generate Mel scale
		generate_subtract_mel_scale();
		// check filter bank
		// 范围为0的滤波器会导致之后计算的对数计算出现NaN，用2e-16提高数值稳定性
		boolean wrongFilter = false;
		for (double[] l : filterBank) {
			boolean allZero = true;
			for (int i = 0; i < l.length; i++) {
				allZero &= (l[i] == 0);
				l[i] = l[i] == 0 ? 2E-16 : l[i];
			}
			wrongFilter = wrongFilter || allZero;
		}
		if (wrongFilter) {
			System.out.println("Empty filters detected in mel frequency basis. " +
					"Try increasing your sampling rate (and fmax) or educing nmfcc.\n");
		}

		// step 2: generate FFT basis
		omega = FFT.getOmega(nFFT);


		hamming_function = new double[frameLength];
		for (int i = 0; i < frameLength; i++) {
			hamming_function[i] = (1 - HAMMING) - HAMMING * Math.cos((2.0 * PI * i) / frameLength);
		}
	}

	private void generate_subtract_mel_scale() {
		double[] hz_points = new double[nFilt + 2];
		double[] enorm = new double[nFilt];
		double[][] ramps = new double[nFilt + 2][nFFT / 2 + 1];
		double mel_max = hzToMel(sampleRate / 2.0);
		filterBank = new double[nFilt][nFFT / 2 + 1];

		hz_points[0] = 0;
		for (int i = 1; i <= nFilt; i++) {
			hz_points[i] = melToHz(i * mel_max / (nFilt + 1));
		}
		double fftFreq = sampleRate / nFFT;
		for (int i = 0; i < nFilt + 2; i++) {
			for (int j = 0; j < nFFT / 2 + 1; j++) {
				ramps[i][j] = hz_points[i] - fftFreq * j;
			}
		}
		// Slaney-style mel is scaled to be approx constant energy per channel
		for (int i = 0; i < nFilt; i++) {
			enorm[i] = 2.0 / (hz_points[i + 2] - hz_points[i]);
			double hz_diff_0 = hz_points[i + 1] - hz_points[i];
			double hz_diff_1 = hz_points[i + 2] - hz_points[i + 1];
			for (int j = 0; j < nFFT / 2 + 1; j++) {
				double lower = -ramps[i][j] / hz_diff_0;
				double upper = ramps[i + 2][j] / hz_diff_1;
				filterBank[i][j] = Math.max(0, Math.min(lower, upper)) * enorm[i];
			}
		}
	}

	private void generate_eval_mel_scale() {
		double[] mel_points = new double[nFilt + 2];
		double[] hz_points = new double[nFilt + 2];
		double[] enorm = new double[nFilt];
		mel_points[0] = 0;
		mel_points[nFilt + 1] = hzToMel(sampleRate / 2.0);
		hz_points[nFilt + 1] = sampleRate / 2;
		for (int i = 1; i <= nFilt; i++) {
			// even distributions
			mel_points[i] = i * mel_points[nFilt + 1] / (nFilt + 1);
			hz_points[i] = melToHz(mel_points[i]);
		}
		// Slaney-style mel is scaled to be approx constant energy per channel
		for (int i = 0; i < nFilt; i++) {
			enorm[i] = 2.0 / (hz_points[i + 2] - hz_points[i]);
		}
		int[] bin = new int[nFilt + 2];
		for (int i = 0; i < nFilt + 2; i++) {
			bin[i] = (int) Math.floor((nFFT + 1) * hz_points[i] / sampleRate);
		}
		// generate Mel filter
		filterBank = new double[nFilt][nFFT / 2 + 1];
		for (int i = 1; i <= nFilt; i++) {
			Arrays.fill(filterBank[i - 1], 0.0);
			for (int k = bin[i - 1]; k < bin[i]; k++) {
				filterBank[i - 1][k] = (double) (k - bin[i - 1]) / (bin[i] - bin[i - 1]);
				filterBank[i - 1][k] *= enorm[i - 1];
			}
			for (int k = bin[i]; k < bin[i + 1]; k++) {
				filterBank[i - 1][k] = (double) (bin[i + 1] - k) / (bin[i + 1] - bin[i]);
				filterBank[i - 1][k] *= enorm[i - 1];
			}
		}
	}

	/**
	 * Mel-Frequency Cepstral Coefficients (MFCCs)
	 *
	 * @param wave audio data
	 */
	public float[][] process(double[] wave) {

		int wave_len = wave.length;

		/* step 1: Pre-emphasis filter, s_n = s_n - k * s_{n-1} */
		for (int i = wave_len - 1; i > 0; i--) {
			wave[i] = wave[i] - PRE_EMPHASISe * wave[i - 1];
		}

		/* step 2: framming window */
		int num_frames = (int) Math.ceil((wave_len - frameLength + frameStep) / frameStep);
		double[][] frames = new double[num_frames][nFFT];
		int indices = 0;
		for (int i = 0; i < num_frames; i++) {
			for (int j = 0; j < frameLength; j++) {
				frames[i][j] = wave[indices] * hamming_function[j];
				indices++;
			}
			indices -= (frameLength - frameStep);
		}

		/* step 3: fourier transform and power spectrum */
		Complex[] fftResult;
		for (int i = 0; i < num_frames; i++) {
			// get fft result
			Complex[] newWave = new Complex[frameLength];
			for (int j = 0; j < frameLength; j++) {
				newWave[j] = new Complex(frames[i][j]);
			}
			int logl = (int) (Math.log(frameLength + 0.01) / Math.log(2));
			int nextLength = 1 << (logl + 2);
			if ((1 << logl) == frameLength) {
				// case 1: fft length is power of 2
				Complex[] omega = FFT.getOmega(frameLength);
				fftResult = FFT.fftRadix2CooleyTukey(newWave, false, omega);
			} else {
				// case 2: fft length is not power of 2
				Complex[] omega = FFT.getOmega(nextLength);
				Complex[] omega2 = FFT.getOmega(frameLength * 2);
				fftResult = FFT.fftChirpZ(newWave, false, omega, omega2);
			}
			// get power Spectrum
			for (int j = 0; j < frameLength; j++) {
				double rl = fftResult[j].getReal();
				double im = fftResult[j].getImaginary();
				frames[i][j] = rl * rl + im * im;
			}
		}

		/* step 4: Mel-frequency cepstral coefficients*/
		double[][] mfccResult = new double[num_frames][nFilt];
//
//		DenseMatrix dmFrames = new DenseMatrix(frames);
//		DenseMatrix dmFilterBank = new DenseMatrix(filterBank);
//		DenseMatrix dm = dmFilterBank.multiplies(dmFilterBank);

		double amin = 1e-10;
		double[] means = new double[num_frames];
		for (int i = 0; i < num_frames; i++) {
			for (int j = 0; j < nFilt; j++) {
				double dotResult = 0;
				for (int k = 0; k < nFFT / 2 + 1; k++) {
					dotResult += frames[i][k] * filterBank[j][k];
				}
				dotResult = 10 * Math.log10(Math.max(amin, dotResult));
				mfccResult[i][j] = dotResult;
				means[i] += dotResult;
			}
			means[i] /= nFilt;
		}
		means[num_frames - 1] /= nFilt;

		/* data process: Zero-centered */
		for (int i = 0; i < num_frames; i++) {
			for (int j = 0; j < nFilt; j++) {
				mfccResult[i][j] -= (means[i] + 1e-8);
			}
		}

		/* step 5: DCT */
		int dctLength = (int) Math.ceil((Math.log(nFilt) / Math.log(2)));
		dctLength = (int) Math.pow(2, dctLength) + 1;
		double[][] result = new double[num_frames][nFeature];
		// can't use math3 because it only support filter number of power of 2
//		FastCosineTransformer dct = new FastCosineTransformer(DctNormalization.ORTHOGONAL_DCT_I);
		for (int i = 0; i < num_frames; i++) {
			double[] padding = new double[dctLength];
			System.arraycopy(mfccResult[i], 0, padding, 0, nFilt);
			double[] dctResult = dctFunc(mfccResult[i]);
			for (int j = 0; j < nFeature; j++) {
				result[i][j] = dctResult[j + 1];
			}
		}
		return doubleToFloat(result);
	}

	private double melToHz(double mel) {
		double hz = 700.0 * (Math.pow(10.0, (mel / 2595.0)) - 1);
		return hz;
	}

	private double hzToMel(double hz) {
		double mel = 2595.0 * Math.log10(1.0 + hz / 700.0);
		return mel;
	}

	private double[] dctFunc(double[] input) {//transform input
		int length = input.length;
		Complex[] newInput = new Complex[length];
		for (int index = 0; index < (length + 1) / 2; index++) {
			newInput[index] = new Complex(input[2 * index]);
		}
		for (int index = (length + 1) / 2; index < length; index++) {
			newInput[index] = new Complex(input[2 * length - 2 * index - 1]);
		}

		//perform fft
		double[] output = DCTMapper.performDct(newInput);
		return output;
	}

	private static float[][] doubleToFloat(double[][] data) {
		int m = data.length;
		int n = data[0].length;
		float[][] result = new float[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				result[i][j] = (float) data[i][j];
			}
		}
		return result;
	}

}
