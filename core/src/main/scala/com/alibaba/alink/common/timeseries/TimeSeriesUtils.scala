package com.alibaba.alink.common.timeseries

import java.lang.{Boolean => JavaBoolean, Double => JavaDouble, Integer => JavaInteger}

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{DiffFunction, LBFGSB}
import com.alibaba.alink.common.linalg.DenseVector
import com.alibaba.alink.operator.common.timeseries.garch.GarchGradientTarget
import com.alibaba.alink.operator.common.timeseries.holtwinter.HoltWintersUtil

object TimeSeriesUtils {


  def toBreezeVector(v: DenseVector): BDV[Double] = {
    new BDV[Double](v.getData)
  }

  def fromBreezeVector(v: BDV[Double]): DenseVector = {
    require(v.stride == 1)
    require(v.offset == 0)
    new DenseVector(v.data)
  }

  /**
    *
    * @param lowerBound  the lower bound
    * @param upperBound  the upper bound
    * @param hyperParams the initial value of alpha, beta and gamma.
    * @param epsilon     the param in grad.
    * @return the optimized coefs.
    */
  def calcHoltWintersLBFGSB(lowerBound: DenseVector,
                            upperBound: DenseVector,
                            hyperParams: DenseVector,
                            levelStart: JavaDouble,
                            trendStart: JavaDouble,
                            seasonalStart: DenseVector,
                            data: DenseVector,
                            seasonal: JavaBoolean,
                            frequency: Integer,
                            epsilon: DenseVector,
                            iterNum: JavaInteger,
                            tolerance: JavaDouble): DenseVector = {
    val solver = new LBFGSB(toBreezeVector(lowerBound), toBreezeVector(upperBound),
      iterNum, tolerance = tolerance)
    val f = new DiffFunction[BDV[Double]] {
      override def calculate(x: BDV[Double]): (Double, BDV[Double]) = {
        val coefs = new DenseVector(x.data)
        val cost = HoltWintersUtil.calcLoss(data, levelStart, trendStart, seasonalStart, coefs, seasonal, frequency)
        val grad = HoltWintersUtil.calcGrad(coefs, seasonal, frequency, levelStart, trendStart, seasonalStart, data,
          epsilon, lowerBound, upperBound)
        (cost, toBreezeVector(grad))
      }
    }
    val optX = solver.minimize(f, toBreezeVector(hyperParams))
    new DenseVector(optX.data)
  }


  def calcGarchLBFGSB(lowerBound: DenseVector,
                      upperBound: DenseVector,
                      garchGradientTarget: GarchGradientTarget,
                      data: Array[Double],
                      iterNum: JavaInteger,
                      tolerance: JavaDouble): DenseVector = {
    val solver = new LBFGSB(toBreezeVector(lowerBound), toBreezeVector(upperBound),
      iterNum, tolerance = tolerance)

    val f = new DiffFunction[BDV[Double]] {
      override def calculate(x: BDV[Double]): (Double, BDV[Double]) = {
        val coefs = new DenseVector(x.data)
        val cost = garchGradientTarget.f(coefs)
        val grad = garchGradientTarget.gradient(coefs)
        (cost, toBreezeVector(grad))
      }
    }
    val optX = solver.minimize(f, toBreezeVector(garchGradientTarget.initParams()))

    new DenseVector(optX.data)
  }

}
