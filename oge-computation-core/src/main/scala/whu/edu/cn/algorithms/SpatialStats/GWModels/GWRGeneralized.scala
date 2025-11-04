package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{DenseMatrix, DenseVector, MatrixSingularException, inv, sum, trace}
import breeze.stats.mean
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.oge.Service

import scala.collection.mutable
import scala.math._
import scala.util.control.Breaks._

class GWRGeneralized(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD){
  private var _tolerance = 1e-5
  private var _maxIter = 20
  private var _family: String = _

  // var for bw selection
  val select_eps = 1e-4
  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _
  var _nameUsed: Array[String] = _

  private val epsilon = 1e-6 // 正则化常数，确保矩阵可逆

  def setParam(tol: Double, iter: Int)={
    _tolerance = tol
    _maxIter = iter
  }

  def setFamily(family: String)={
    _family = family
  }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true, family: String = "poisson"): Double = {
    _family = family

    println("Selecting bandwidth...")
    // 检查数据量大小
    if (_rows > 1500) {
      println("Take a cup of tea and have a break, it will take a few minutes.")
      println("          -----A kind suggestion from GWmodel Lab")
    }

    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }


  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Double = _disMax, lower: Double = _disMax / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1
    } catch {
      case e: Throwable => {
        println(f"meet error: ${e.getMessage}")
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Int = _rows - 1, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1.toInt
    } catch {
      case e: Throwable => {
        println(f"meet error ${e.getMessage}")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }

    bw
  }

  private def bandwidthAICc(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = ggwr_fit(weight = newWeight)
    //    val yhat = results._2
    //    val residual = results._3
    val shat = results._4
    val trS = trace(shat)
    val n = _rows
    val llik = results._5
    // 计算GAICc
    val gaicc = -2 * llik + 2 * trS + 2 * trS * (trS + 1) / (n - trS - 1)
    val bw_print = if(_adaptive) bw.toInt else bw.formatted("%.2f")
    println(f"Bandwidth: ${bw_print}, AICc value: ${gaicc.formatted("%.5f")}")
    gaicc
  }

  private def bandwidthCV(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val cvWeight = modifyWeightCV(newWeight)
    val results = ggwr_fit(weight = cvWeight)
    val residual = results._3

    val cvDeviance =  residual.t * residual
    val bw_print = if(_adaptive) bw.toInt else bw.formatted("%.2f")
    println(f"Bandwidth: ${bw_print}, CV value: ${cvDeviance.formatted("%.5f")}")
    cvDeviance
  }

  private def modifyWeightCV(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.zipWithIndex.map { case (t, idx) =>
      val modifiedVector = t.copy
      modifiedVector(idx.toInt) = 0 // 只将当前点的权重设为0
      modifiedVector
    }
  }

  def ggwr_fit(X: DenseMatrix[Double] = _dmatX, Y: DenseVector[Double] = _dvecY, weight: RDD[DenseVector[Double]] = _spWeight,
               tol: Double = _tolerance, maxIter: Int = _maxIter)
  = {

    // 初始化
    var betas = Array.fill(_rows)(DenseVector.zeros[Double](_cols))
    var y_adj = DenseVector.zeros[Double](_rows)
    var yhat = DenseVector.zeros[Double](_rows)
    var residual = DenseVector.zeros[Double](_rows)
    var shat = DenseMatrix.zeros[Double](_rows, _rows)
    var llik = 0.0
    var wt2 = DenseVector.ones[Double](_rows)
    _family match {
        case "poisson" => {
            // 检查数据有效性
            if (Y.toArray.exists(_ < 0)) {
                throw new IllegalArgumentException("negative values not allowed for the 'Poisson' family")
            }
            // 初始化mu和eta
            var mu = Y + 0.1
            var nu = mu.map(math.log)
            // 初始化权重

            var old_llik = Double.NegativeInfinity
            //var llik = 0.0

            // IRWLS迭代
            breakable {
                for (iter <- 0 until maxIter) {
                    // 计算工作变量
                    y_adj = nu + (Y - mu) / mu
                    // 计算加权最小二乘
                    val xtw = weight.map(w_sp => {
                        val each_col_mat = _dvecX.map(t => t * w_sp * wt2).flatMap(_.toArray)
                        new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
                    })
                    // 更新beta
                    betas = xtw.map(xtw => {
                        try {
                            val xtwx = xtw * X
                            val xtwy = xtw * y_adj
                            // 使用QR分解求解
                            val qr = breeze.linalg.qr.reduced(xtwx)
                            val beta = breeze.linalg.inv(qr.r) * (qr.q.t * xtwy)
                            beta
//                          val xtwx_inv = inv(xtwx)
//                          xtwx_inv * xtwy
                        } catch {
                            case e: MatrixSingularException =>
                                try {
                                    val regularized = inv(regularizeMatrix(xtw * X))
                                    regularized * xtw * y_adj
                                } catch {
                                    case e: Exception =>
                                        throw new IllegalStateException("Matrix inversion failed")
                                }
                        }
                    }).collect()

                    // 更新eta和mu
                    nu = getYHat(X, betas)
                    mu = nu.map(math.exp)
                    // 计算对数似然
                    old_llik = llik
                    llik = Y.toArray.zip(mu.toArray).map { case (y, m) =>
                        if (y > 0) {
                          breeze.stats.distributions.Poisson(m).logProbabilityOf(y.toInt)
                        } else {
                            -m
                        }
                    }.sum
                  // 添加迭代输出
                  if (iter == 0) {
                    println(s"Iteration    Log-Likelihood")
                    println("=========================")
                  }
                  println(f"${iter}%8d    ${llik}%10.3f")

                    // 检查收敛性
                    if (math.abs((old_llik - llik) / llik) < tol) {
                        yhat = mu
                        residual = Y - mu
                        break()
                    }

                    // 更新权重
                    wt2 = mu
                    yhat = mu
                    residual = Y - mu
                }
            }

            // 计算帽子矩阵
            val ci: RDD[DenseMatrix[Double]] = weight.map(w_sp => {
                val each_col_mat = _dvecX.map(t => t * w_sp * wt2).flatMap(_.toArray)
                val xtw = new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
                try {
                    val xtwx_inv = inv(xtw * X)
                    xtwx_inv * xtw
                } catch {
                    case e: MatrixSingularException =>
                        val regularized = inv(regularizeMatrix(xtw * X))
                        regularized * xtw
                }
            })

            val ci_idx = ci.zipWithIndex
            val si = ci_idx.map(t => {
                val a = X(t._2.toInt, ::).inner.toDenseMatrix
                val b = t._1.toDenseMatrix
                a * b
            })

            shat = DenseMatrix.create(
                rows = si.collect().length,
                cols = si.collect().length,
                data = si.collect().flatMap(_.toArray)
            )

            // 计算诊断统计量
            def diag(value: DenseVector[Double]) ={
              val n = value.length
              val res = DenseMatrix.eye[Double](n)
              for(i <- 0 until n) res(i,i) = value(i)
              res
            }
            val trS = trace(shat)
            val trStS = trace(shat * diag(wt2) * shat.t * diag(1.0 / wt2))
            val edf = _rows - 2 * trS + trStS
        }

        case "binomial" => {
            // 二项回归的实现
            // TODO: 实现二项回归的IRWLS
          // 检查数据有效性
          if (Y.toArray.exists(y => y != 0 && y != 1)) {
            throw new IllegalArgumentException("values must be 0 or 1 for the 'binomial' family")
          }

          // 初始化mu和eta
          var mu = Y.map(y => 0.5)
          var nu = DenseVector.zeros[Double](_rows) //mu.map(m => math.log(m / (1 - m)))
          var ones = DenseVector.ones[Double](_rows)

          // 初始化权重
          var wt2 = DenseVector.ones[Double](_rows)
          var old_llik = Double.NegativeInfinity

          // IRWLS迭代
          breakable {
            for (iter <- 0 until maxIter) {
              // 计算工作变量
              //y_adj = nu + (Y - ones * mu) / (ones * mu * (ones - mu))
              y_adj = nu + (Y - mu) / mu.map(m => m * (1 - m))

              // 计算加权最小二乘
              val xtw = weight.map(w_sp => {
                val each_col_mat = _dvecX.map(t => t * w_sp * wt2).flatMap(_.toArray)
                new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
              })

              // 更新beta
              betas = xtw.map(xtw => {
                try {
                  val xtwx = xtw * X
                  val xtwy = xtw * y_adj
                  // 使用QR分解求解
                  val qr = breeze.linalg.qr.reduced(xtwx)
                  val beta = breeze.linalg.inv(qr.r) * (qr.q.t * xtwy)
                  beta
                } catch {
                  case e: MatrixSingularException =>
                    try {
                      val regularized = inv(regularizeMatrix(xtw * X))
                      regularized * xtw * y_adj
                    } catch {
                      case e: Exception =>
                        throw new IllegalStateException("Matrix inversion failed")
                    }
                }
              }).collect()

              // 更新eta和mu
              nu = getYHat(X, betas)
              //                    mu = nu.map(math.exp)/(ones + nu.map(math.exp))
              mu = nu.map(t => math.exp(t) / (1 + math.exp(t)))

              // 计算对数似然
              old_llik = llik
              llik = Y.toArray.zip(mu.toArray).map { case (y, m) =>
                y * math.log(m) + (1 - y) * math.log(1 - m)
              }.sum

              if (llik.isNaN||llik.isInfinity) llik = old_llik

              // 添加迭代输出
              if (iter == 0) {
                println(s"Iteration    Log-Likelihood")
                println("=========================")
              }
              println(f"${iter}%8d    ${llik}%10.3f")

              // 检查收敛性
              if (math.abs((old_llik - llik) / llik) < tol) {
                yhat = mu
                residual = Y - mu
                break()
              }

              // 更新权重
              //wt2 = ones * mu * (ones - mu)
              wt2 = mu.map(m => m * (1 - m))
              yhat = mu
              residual = Y - mu
            }
          }

          // 计算帽子矩阵
          val ci: RDD[DenseMatrix[Double]] = weight.map(w_sp => {
            val each_col_mat = _dvecX.map(t => t * w_sp * wt2).flatMap(_.toArray)
            val xtw = new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
            try {
              val xtwx_inv = inv(xtw * X)
              xtwx_inv * xtw
            } catch {
              case e: MatrixSingularException =>
                val regularized = inv(regularizeMatrix(xtw * X))
                regularized * xtw
            }
          })

          val ci_idx = ci.zipWithIndex
          val si = ci_idx.map(t => {
            val a = X(t._2.toInt, ::).inner.toDenseMatrix
            val b = t._1.toDenseMatrix
            a * b
          })

          shat = DenseMatrix.create(
            rows = si.collect().length,
            cols = si.collect().length,
            data = si.collect().flatMap(_.toArray)
          )

          // 计算诊断统计量
          def diag(value: DenseVector[Double]) = {
            val n = value.length
            val res = DenseMatrix.eye[Double](n)
            for (i <- 0 until n) res(i, i) = value(i)
            res
          }

          val trS = trace(shat)
          val trStS = trace(shat * diag(wt2) * shat.t * diag(1.0 / wt2))
          val edf = _rows - 2 * trS + trStS

        }
    }
    (betas, yhat, residual, shat, llik, wt2)
  }

  protected def getYHat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val betas_idx = betas.zipWithIndex
    val yhat = betas_idx.map(t => {
      sum(t._1 * X(t._2, ::).inner)
    })
    DenseVector(yhat)
  }

  def regularizeMatrix(matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val eye = DenseMatrix.eye[Double](matrix.rows)
    matrix + eye * epsilon //添加epsilon
  }

  protected def ggwrDiagnostic(Y: DenseVector[Double], yhat: DenseVector[Double],
                               shat: DenseMatrix[Double], wt2: DenseVector[Double], family: String = "poisson"): String = {
    var diagString = ""
    val n = _rows
    val trS = trace(shat)
    val gwDeviance = if(family == "poisson") Y.toArray.zip(wt2.toArray).map{case(y, m) => {
      if(y!= 0) 2 * (y * (log(y / m) - 1) + m) else 2 * m
    }}.sum
    else Y.toArray.zip(yhat.toArray).map { case (y, mu) =>
      if (y == 0) {
        if (mu < 1e-10) 0.0 else 2 * math.log(1 / (1 - mu))
      } else if (y == 1) {
        if (mu > 1 - 1e-10) 0.0 else 2 * math.log(1 / mu)
      } else {
        0.0 // 这种情况不应该发生，因为已经检查过y只能是0或1
      }
    }.sum
    val AIC = gwDeviance + 2 * trS
    val AICc = gwDeviance + 2 * trS + 2 * trS * (trS + 1) / (n - trS - 1)
    val mu_null = mean(Y)
    val null_deviance = if(family == "poisson")2 * Y.toArray.zip(Array.fill(Y.length)(mu_null)).map { case (y, m) =>
      if (y > 0) y * math.log(y / m) - (y - m) else m
    }.sum
    else -2 * Y.toArray.zip(Array.fill(Y.length)(mu_null)).map { case (y, p) =>
      y * math.log(p) + (1 - y) * math.log(1 - p)
    }.sum
    val pseudoR2 = 1 - gwDeviance/null_deviance
    diagString += "*****************************Diagnostic information******************************\n" +
      f"Number of data points: $n \nGW Deviance: $gwDeviance%.5f\nAIC: $AIC%.5f\nAICc: $AICc%.5f\nPseudo R-square value: $pseudoR2%.7f\n" +
      "*********************************************************************************\n"
    diagString
  }

  //---------------------------------------------------------------------------------------------

  // gw poisson reg
  def GWPR(adaptive: Boolean, approach: String, kernel: String, bandwidth: Double, tol: Double, maxIter: Int):
  (Array[((String, (Geometry, mutable.Map[String, Any])), Int)], String) ={
    val bw = if (bandwidth <=0) bandwidthSelection(adaptive = adaptive, approach = approach, kernel = kernel,family = "poisson") else bandwidth
    //    println(f"Bandwidth: $bw")

    _kernel = kernel
    _adaptive = adaptive
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = ggwr_fit(weight = newWeight, tol = tol, maxIter = maxIter)
    val betas = results._1
    val arr_yhat = results._2.toArray
    val arr_residual = results._3.toArray
    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> arr_yhat(t._2))
      t._1._2._2 += ("residual" -> arr_residual(t._2))
    })
    if (_nameUsed == null) {
      _nameUsed = _nameX
    }
    val name = Array("Intercept") ++ _nameUsed
    for (i <- 0 until _cols) {
      shpRDDidx.map(t => {
        t._1._2._2 += (name(i) -> betas(t._2)(i))
      })
    }
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    val fitFormula = _nameY + " ~ " + _nameUsed.mkString(" + ")
    var fitString = "\n*********************************************************************************\n" +
      "*             Results of Geographically Weighted Poisson Regression             *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Formula: $fitFormula" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n"
    fitString += ggwrDiagnostic(Y = _dvecY, yhat = results._2, shat = results._4, wt2 = results._6, family = "poisson")

    //    println(f"Yhat: ${arr_yhat.toList}")
    (shpRDDidx, fitString)
  }

  // gw binomial reg
  def GWBR(adaptive: Boolean, approach: String, kernel: String, bandwidth: Double, tol: Double, maxIter: Int):
  (Array[((String, (Geometry, mutable.Map[String, Any])), Int)], String) ={
    val bw = if(bandwidth <= 0) bandwidthSelection(adaptive = adaptive, approach = approach, kernel = kernel,family = "binomial") else bandwidth
    //    println(f"Bandwidth: $bw")

    _kernel = kernel
    _adaptive = adaptive
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = ggwr_fit(weight = newWeight, tol = tol, maxIter = maxIter)
    val betas = results._1
    val arr_yhat = results._2.toArray
    val arr_residual = results._3.toArray
    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> arr_yhat(t._2))
      t._1._2._2 += ("residual" -> arr_residual(t._2))
    })
    if (_nameUsed == null) {
      _nameUsed = _nameX
    }
    val name = Array("Intercept") ++ _nameUsed
    for (i <- 0 until _cols) {
      shpRDDidx.map(t => {
        t._1._2._2 += (name(i) -> betas(t._2)(i))
      })
    }
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    val fitFormula = _nameY + " ~ " + _nameUsed.mkString(" + ")
    var fitString = "\n*********************************************************************************\n" +
      "*             Results of Geographically Weighted Binomial Regression            *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Formula: $fitFormula" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw%.2f\n"
    fitString += ggwrDiagnostic(Y = _dvecY, yhat = results._2, shat = results._4, wt2 = results._6, family = "binomial")
    //    println(f"Yhat: ${arr_yhat.toList}")
    (shpRDDidx,fitString)
  }


}

object GWRGeneralized{
  /**
   *
   * @param featureRDD  原始数据
   * @param propertyY   因变量
   * @param propertiesX 自变量
   * @param bandwidth   给定带宽，若值为非正数则自动优选带宽（默认为-1）
   * @param family      分布族，包括"poisson"（默认）和"binomial"，分别对应GWPR和GWBR
   * @param kernel      核函数
   * @param approach    带宽优选指标，包括"AICc"（默认）和"CV"
   * @param adaptive    带宽类型，Boolean型变量，true（默认）对应适应带宽，false对应固定带宽
   * @param tolerance   迭代阈值，默认为1e-5
   * @param maxIter     最大迭代次数，默认为20
   * @return
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double = -1, family: String = "poisson", kernel: String = "gaussian", approach: String = "AICc",adaptive: Boolean = true,
          tolerance: Double = 1e-05, maxIter: Int = 20):
  RDD[((String, (Geometry, mutable.Map[String, Any])), Int)] ={
    val model = new GWRGeneralized(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    //model.setParam(tolerance, maxIter)
    model.setFamily(family)

    val result = family match {
      case "poisson" => model.GWPR(adaptive, approach, kernel, bandwidth, tol = tolerance, maxIter = maxIter)
      case "binomial" => model.GWBR(adaptive, approach, kernel, bandwidth, tol = tolerance, maxIter = maxIter)
      case _ => throw new IllegalStateException("Invalid family. Only \"poisson\" and \"binomial\" are valid.")
    }
    Service.print(result._2, "Basic GGWR calculation with bandwidth and variables auto selection", "String")
    sc.makeRDD(result._1)
  }
}