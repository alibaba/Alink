package com.alibaba.alink.common.utils;

import com.alibaba.alink.operator.common.dataproc.FFT;
import org.apache.commons.math3.complex.Complex;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for Fast Fourier Transformation(FFT).
 */
public class FFTTest {
	@Test
	public void testRadix2CooleyTukey() throws Exception {
		double[] input = new double[] {1.0, 2.0, 1.0, 2.0};
		int length = input.length;
		Complex[] complexes = new Complex[length];
		for (int i = 0; i < length; i++) {
			complexes[i] = new Complex(input[i]);
		}

		Complex[] omega = FFT.getOmega(length);
		Complex[] fft = FFT.fftRadix2CooleyTukey(complexes, false, omega);
		Complex[] ifft = FFT.fftRadix2CooleyTukey(fft, true, omega);

		for (int i = 0; i < length; i++) {
			assertTrue(complexes[i].subtract(ifft[i]).abs() < 1e-10);
		}
	}

	@Test
	public void testChirpZ() throws Exception {
		double[] input = new double[] {1.0, 2.0, 1.0, 2.0, 1.0};
		int length = input.length;
		Complex[] complexes = new Complex[length];
		for (int i = 0; i < length; i++) {
			complexes[i] = new Complex(input[i]);
		}

		int logl = (int) (Math.log(length + 0.01) / Math.log(2));
		int nextLength = 1 << (logl + 2);
		Complex[] omega = FFT.getOmega(nextLength);
		Complex[] omega2 = FFT.getOmega(length * 2);
		Complex[] fft = FFT.fftChirpZ(complexes, false, omega, omega2);
		Complex[] ifft = FFT.fftChirpZ(fft, true, omega, omega2);

		for (int i = 0; i < length; i++) {
			assertTrue(complexes[i].subtract(ifft[i]).abs() < 1e-10);
		}
	}

}