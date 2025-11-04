package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{DenseMatrix, DenseVector, MatrixSingularException, inv, trace}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.getSpatialweight
import whu.edu.cn.oge.Service

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.control.Breaks

class MGWR(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbasic(inputRDD) {

  val maxIter:Int=10000
  var bwOptiArr:Array[Array[Double]]=_
  var bwOptiMap:Array[mutable.Map[Int, Double]]=_

  // 初始带宽组
  var bw0: Double = 0.0
  var bwArr0:Array[Double]=_
  var fitArr0 : Array[(Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseMatrix[Double]])] = _
  var _approach:String=_

  var eps0:Double = 0

  var mbetas:Array[Array[Double]]=_

  var isPrint: Boolean = false

//  protected def setKernel(kernel:String) ={
//    _kernel = kernel
//  }
//
//  protected def setApproach(approach: String) = {
//    _approach = approach
//  }
//
//  protected def setAdaptive(adaptive: Boolean) = {
//    _adaptive = adaptive
//  }
//
//  protected def setEps(eps: Double) ={
//    eps0 = eps
//  }

  protected def setParams(kernel:String, approach: String, adaptive: Boolean, eps: Double) ={
    _kernel = kernel
    _approach = approach
    _adaptive = adaptive
    eps0 = eps
  }

  // 这个地方改成对每一个变量逐个进行回归
  def fitAll(kernel: String, approach: String, adaptive: Boolean)={
    println("Initializing...")
    _kernel=kernel
    _adaptive=adaptive
    bwArr0 = (0 until _cols).map(t => 0.0).toArray
    val Res = (0 until _cols).map{ i =>
      val varname = if(i==0) "Intercept" else _nameX(i-1)
      val yName = _nameY
      println(f"Now select an optimum bandwidth for the model: $yName ~ $varname")

      val mXi = _dvecX(i)
      // 构造当前变量的局部回归设计矩阵
      val arrmXi = (0 to 0).map(t => mXi).toArray
      //      val onesVector = DenseVector.ones[Double](_rows)
      val vecmXi = arrmXi
      val matmXi = DenseMatrix.create(rows = _rows, cols = 1, data = vecmXi.flatMap(_.toArray))

      // 这里为当前变量单独优选带宽
      isPrint = true
      val localBw = if (_adaptive) {
        adaptiveBandwidthSelectionLocal(X = matmXi, Y = _dvecY)
      } else {
        fixedBandwidthSelectionLocal(X = matmXi, Y = _dvecY)
      }
      bwArr0(i) = localBw
      val newWeight = if (_adaptive) {
        setWeightLocal(round(localBw))
      } else {
        setWeightLocal(localBw)
      }
//      println("fit ready")
      val res_fit = fitLocal(matmXi, _dvecY, weight = newWeight)
      (localBw, res_fit)
    }.toArray

    bwArr0 = Res.map(_._1)
    fitArr0 = Res.map(_._2)

    isPrint = false
    bw0 = if(_adaptive){
      adaptiveBandwidthSelectionLocal(X = _dmatX, Y = _dvecY)
    }else{
      fixedBandwidthSelectionLocal(X = _dmatX, Y = _dvecY)
    }


//    println(f"bw inifial: ${bwArr0.toList}")

    // -------------------------
    //    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    //    bw0=bwselect
    //    val newWeight = if (_adaptive) {
    //      setWeight(round(bwselect), _kernel, _adaptive)
    //    } else {
    //      setWeight(bwselect, _kernel, _adaptive)
    //    }
    //    println(f"Initial bandwidth: $bw0")
    //    fitFunction(weight = newWeight)

  }

  def backfitting(iter:Int)={
    // var.n = _cols, dp.n = _rows
    fitAll(_kernel, _approach, _adaptive)

    val weight0 = if (_adaptive) {
      setWeightLocal(round(bw0))
    } else {
      setWeightLocal(bw0)
    }
    val res0 = fitLocal(_dmatX, _dvecY, weight = weight0)

    var betas0 = res0._1
    var yhat0 = res0._2
    var resid0 = res0._3
    var Shat = res0._4
    var C0 = res0._5
    println("Initial Selection Finished")

    var rss0 = resid0.toArray.map(t => t*t).sum

    val S_arrays = new Array[DenseMatrix[Double]](_cols)
    for(i<- 0 until _cols){
      S_arrays(i) = DenseMatrix.zeros[Double](_rows,_rows)
    }

    //初始化
    for(i <- 0 until _cols; j <- 0 until _rows){
      val xji = _dmatX(j,i)
      val Cj = C0(j)
      val term = xji * Cj(i,::).t
      S_arrays(i)(j,::) := term.t
    }

    val betasT: Array[Array[Double]] = betas0.map(_.toArray).transpose

    val matFi = DenseMatrix.zeros[Double](_rows,_cols)
    for(i<-0 until _cols){
      matFi(::,i) := DenseVector(betasT(i)) *:* _dvecX(i)
    }
    val matFi_old = matFi.copy/*DenseMatrix.zeros[Double](_rows,_cols)*/
//    for (r <- 0 until _rows; c <- 0 until _cols) {
//      matFi_old(r, c) = matFi(r, c)
//    }

    val vecBw = DenseVector.zeros[Double](_cols)
    val matBetas = DenseMatrix.zeros[Double](_rows, _cols)
    // initialization
    for(i <- 0 until _rows; j <- 0 until _cols){
      matBetas(i,j) = betas0(i)(j)
    }

    // calculation
    var iter_present = 1
    val iter_max = if(iter>0) iter else maxIter
    var is_converged = false
    while(!is_converged && iter_present <= iter_max){
      println(f"---------------------------iteration $iter_present---------------------------")
      for (i <- 0 until _cols) {
        val varname = if(i==0)"Intercept" else _nameX(i-1)
        println(f"Select a bandwidth for variable ${i+1}/${_cols}: $varname")

        val mXi = _dvecX(i)
        val fi = matFi(::, i)
        val yi = resid0 + fi

        val arrmXi = (0 to 0).map(t => mXi).toArray
        //      val onesVector = DenseVector.ones[Double](_rows)
        val vecmXi = arrmXi
        val matmXi = DenseMatrix.create(rows = _rows, cols = 1, data = vecmXi.flatMap(_.toArray))

        isPrint = false
        val localBw = if(_adaptive){
          adaptiveBandwidthSelectionLocal(X = matmXi, Y = yi)
        }else{
          fixedBandwidthSelectionLocal(X = matmXi, Y = yi)
        }
        val localWeight = if (_adaptive) {
          setWeightLocal(round(localBw))
        } else {
          setWeightLocal(localBw)
        }
        val localGWRRes = fitLocal(matmXi,yi,localWeight)

        val betai = localGWRRes._1.map(t =>t(0))
        val Si = localGWRRes._4
        val S_arrayi = S_arrays(i)
        S_arrays(i) = Si * S_arrayi + Si - Si * Shat
        Shat = Shat - S_arrayi + S_arrays(i)
        println(f"Newly selected bandwidth for $varname : ${localBw.toInt}")

        //update fi
        matFi(::, i) := DenseVector(betai) *:* mXi
        vecBw(i) = localBw
        matBetas(::, i) := DenseVector(betai)
        resid0 = _dvecY - DenseVector((0 until _rows).map(t => matFi(t, ::).inner.sum).toArray)

        println("--------------------------------------------")
      }

      val resid1 = _dvecY - DenseVector((0 until _rows).map(t => matFi(t, ::).inner.sum).toArray)
      val rss1 = resid1.map(t =>t*t).sum
      is_converged = converge(matFi,matFi_old)
//      is_converged = calc_dCVR(rss0, rss1)
      matFi_old := matFi.copy
//      for(r <- 0 until _rows;c <- 0 until _cols){
//        matFi_old(r,c) = matFi(r,c)
//      }
      iter_present = iter_present+1
      rss0 = rss1
    }
    println("Backfitting complete")
    val res = (matFi,vecBw,matBetas,Shat)
    res
  }

  def regress(iter: Int)={
    val bf = backfitting(iter)
    val matFi = bf._1
    val vecBw = bf._2
    val matBetas = bf._3
    val shat = bf._4

    // get yhat, residuals
    val yhat = (0 until _rows).map(t=>{
      matFi(t,::).inner.sum
    }).toArray
    val residuals = _dvecY - DenseVector(yhat)
    val rss = residuals.map(t=>t*t).sum
    val n = _rows
    val p = _cols - 1
//    val diag = calDiagnostic(_dmatX,_dvecY,residuals,shat)
//    println(diag)
//    println(f"yhat: ${yhat.toList}")
//    println(f"residuals: $residuals")
//    println(f"bandwidth: $vecBw")
    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.map(t => {
      t._1._2._2 += ("yhat" -> yhat)
      t._1._2._2 += ("residual" -> residuals)
    })
    if (_nameUsed == null) {
      _nameUsed = _nameX
    }
    val name = Array("Intercept") ++ _nameUsed
    for (i <- 0 until _cols) {
      shpRDDidx.map(t => {
        t._1._2._2 += (name(i) -> matBetas(::,i))
      })
    }
    val fitFormula = _nameY + " ~ " + _nameUsed.mkString(" + ")
    val bw_type = if(_adaptive) "Adaptive" else "Fixed"
    val kernel = _kernel
    // require modification when intercept == false
    var mat_bw = DenseMatrix.zeros[String](2,_cols)
    for(i<- 0 until _cols){
      if(i ==0){
        mat_bw(0,i) = "Intercept"
      }
      else{
        mat_bw(0,i) = _nameUsed(i-1)
      }
      mat_bw(1,i) = if(_adaptive)round(vecBw(i)).toString else vecBw(i).formatted("%.2f")
    }
//    var str_bw = "\n"
//    for(i <- 0 until _cols){
//      val varname = if (i == 0) "Intercept" else _nameUsed(i-1)
//      str_bw += f"$varname: ${vecBw(i).toString}\n"
//    }
    val fitString = "\n*********************************************************************************\n" +
      "*               Results of Geographically Weighted Regression                   *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Formula: $fitFormula" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"\n${mat_bw}\n" +
      calDiagnostic(_dmatX,_dvecY,residuals,shat)
    (shpRDDidx.map(t => t._1), fitString)
    //(matBetas,yhat,residuals,shat,fitString)

  }

  protected def converge(Fi: DenseMatrix[Double], FiOld: DenseMatrix[Double]): Boolean = {
    val n = _rows
    val numerator = (Fi -:- FiOld).map(t => t * t).sum / n
    val denominator = (0 until _rows).map(t => {
      pow(Fi(t, ::).inner.sum, 2)
    }).sum
    val SOCf = sqrt(numerator / denominator)
    println(f"SOC-f: $SOCf")
    if (SOCf <= eps0) {
      true
    } else {
      false
    }
  }

  protected def calc_dCVR(rss0: Double, rss1: Double): Boolean = {
    val dCVR = math.sqrt(math.abs(rss1 - rss0) / rss1)
    println(f"dCVR: $dCVR")
    if (dCVR <= eps0) {
      true
    } else {
      false
    }
  }

  // below is functions of local GWR, here kernel, adaptive and approach are same as those of mgwr.
  // (1) bandwidth selection
  private def fixedBandwidthSelectionLocal(upper: Double = _disMax, lower: Double = _disMax / 5000.0, X: DenseMatrix[Double], Y: DenseVector[Double]): Double = {
    var bw: Double = lower
    var approachfunc: (DenseMatrix[Double], DenseVector[Double], Double) => Double = bandwidthAICcLocal
    if (_approach == "CV") {
      approachfunc = bandwidthCVLocal
    }
    try {
      val re = goldenSelectionLocal(lower, upper, eps = select_eps, findMax = false,
        X, Y, function = approachfunc)
      bw = re._1
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
        bw = fixedBandwidthSelectionLocal(upper, low, X, Y)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelectionLocal(upper: Int = _rows - 1, lower: Int = 20, X: DenseMatrix[Double], Y: DenseVector[Double]): Int = {
    var bw: Int = lower
    var approachfunc: (DenseMatrix[Double], DenseVector[Double], Double) => Double = bandwidthAICcLocal
    if (_approach == "CV") {
      approachfunc = bandwidthCVLocal
    }
    try {
      val re = goldenSelectionLocal(lower, upper, eps = select_eps, findMax = false, X, Y, function = approachfunc)
      bw = re._1.toInt
    } catch {
      case e: MatrixSingularException => {
        println("meet matrix singular error")
        val low = lower + 1
        bw = adaptiveBandwidthSelectionLocal(upper, low, X, Y)
      }
    }
    bw
  }


  /**
   *
   * @param lower    优化值的下限
   * @param upper    优化值的上限
   * @param eps      优化条件，迭代相差小于eps时退出
   * @param findMax  寻找最大值为true，寻找最小值为false，默认为true
   * @param function 获取（更新）优化值的函数，需要为输入double，输出double的类型
   * @return 优化结果
   */
  protected def goldenSelectionLocal(lower: Double, upper: Double, eps: Double = 1e-10, findMax: Boolean = true,
                           X: DenseMatrix[Double], Y: DenseVector[Double], function: (DenseMatrix[Double], DenseVector[Double], Double) => Double):
  (Double, Array[Double], Array[Double], Array[Double]) = {
    var iter: Int = 0
    val max_iter = 1000
    val loop = new Breaks
    val ratio: Double = (sqrt(5) - 1) / 2.0
    var a = lower + 1e-12
    var b = upper - 1e-12
    var step = b - a
    var p = a + (1 - ratio) * step
    var q = a + ratio * step
    var f_a = function(X, Y, a)
    var f_b = function(X, Y, b)
    var f_p = function(X, Y, p)
    var f_q = function(X, Y, q)
    val opt_iter = new ArrayBuffer[Double]()
    val opt_val = new ArrayBuffer[Double]()
    val opt_res = new ArrayBuffer[Double]()
    //    println(f_a,f_b,f_p,f_q)
    loop.breakable {
      while (abs(f_a - f_b) >= eps && iter < max_iter) {
        if (findMax) {
          if (f_p > f_q) {
            b = q
            f_b = f_q
            q = p
            f_q = f_p
            step = b - a
            p = a + (1 - ratio) * step
            f_p = function(X, Y, p)
          } else {
            a = p
            f_a = f_p
            p = q
            f_p = f_q
            step = b - a
            q = a + ratio * step
            f_q = function(X, Y, q)
          }
        }
        else {
          if (f_p < f_q) {
            b = q
            f_b = f_q
            q = p
            f_q = f_p
            step = b - a
            p = a + (1 - ratio) * step
            f_p = function(X, Y, p)
          } else {
            a = p
            f_a = f_p
            p = q
            f_p = f_q
            step = b - a
            q = a + ratio * step
            f_q = function(X, Y, q)
          }
        }
        iter += 1
        opt_iter += iter
        //        opt_val += (b + a) / 2.0
        //        opt_res += function(sc, (b + a) / 2.0)
        opt_val += p
        opt_res += f_p
        //        println(s"Iter: $iter, optimize value: $p, result is $f_p")
        if (abs(a - b) < eps / 10) {
          loop.break()
        }
      }
    }
    opt_iter += (iter + 1)
    opt_val += (b + a) / 2.0
    opt_res += function(X, Y, (b + a) / 2.0)
    //    println((b + a) / 2.0, function((b + a) / 2.0))
    //    ((b + a) / 2.0, opt_iter.toArray, opt_val.toArray)
    ((b + a) / 2.0, opt_iter.toArray, opt_val.toArray, opt_res.toArray)
  }

  private def bandwidthAICcLocal(X: DenseMatrix[Double], Y: DenseVector[Double], bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeightLocal(round(bw))
    } else {
      setWeightLocal(bw)
    }
    val results = fitLocal(X, Y, weight = newWeight)
    val residual = results._3
    val shat = results._4
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = X.rows
    val aicc = n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
    if(isPrint) {
      if (_adaptive) {
        println(f"Adaptive bandwidth: ${round(bw)}, AICc value: $aicc")
      }
      else {
        println(f"Fixed bandwidth: ${bw}, AICc value: $aicc")
      }
    }
    aicc
  }

  private def bandwidthCVLocal(X: DenseMatrix[Double], Y: DenseVector[Double], bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeightLocal(round(bw))
    } else {
      setWeightLocal(bw)
    }
    val cvWeight = modifyWeightCVLocal(newWeight)
    val results = fitLocal(X, Y, weight = cvWeight)
    val residual = results._3
    val cv_res = residual.toArray.map(t => t * t).sum
    if(isPrint) {
      if(_adaptive){
        println(f"Adaptive bandwidth: ${round(bw)}, CV deviance: $cv_res")
      }
      else {
        println(f"Fixed bandwidth: ${bw}, CV deviance: $cv_res")
      }
    }
    cv_res
  }

  private def modifyWeightCVLocal(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.zipWithIndex.map { case (t, idx) =>
      val modifiedVector = t.copy
      modifiedVector(idx.toInt) = 0 // 只将当前点的权重设为0
      modifiedVector
    }
  }

  // (2) bw -> set weight
  protected def setWeightLocal(bw: Double): RDD[DenseVector[Double]] = {
    getSpatialweight(_dist, bw = bw, _kernel, _adaptive)
  }

  // local fit
  protected def fitLocal(X: DenseMatrix[Double], Y: DenseVector[Double], weight: RDD[DenseVector[Double]]):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseMatrix[Double]]) = {
    val rows = X.rows
    val cols = X.cols
    val vecX = (0 until cols).map(t => {
      val vec = X(::,t)
      vec
    }).toArray

    val xtw = weight.map(w => {
      val each_col_mat = vecX.map(t => t * w).flatMap(_.toArray)
      new DenseMatrix(rows = rows, cols = cols, data = each_col_mat).t
    })
    //    val xtw0=xtw.collect()
    val betas = xtw.map(xtw => {
      try {
        val xtwx = xtw * X
        val xtwy = xtw * Y
        val xtwx_inv = inv(xtwx)
        xtwx_inv * xtwy
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(xtw * X))
            regularized * xtw * Y
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          println(s"An unexpected error occurred: ${e.getMessage}")
          DenseVector.zeros[Double](cols)
      }
    })
    //    val betas0 = betas.collect()
    //    betas0.foreach(println)
    val ci: RDD[DenseMatrix[Double]] = xtw.map(t => {
      try {
        val xtwx_inv = inv(t * X)
        xtwx_inv * t // 继续进行矩阵乘法
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(t * X)) // 先进行正则化，再求逆
            regularized * t
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          throw new RuntimeException("An unexpected error occurred during matrix computation.")
      }
    })
    val ci_idx = ci.zipWithIndex
    //    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => {
      val a = X(t._2.toInt, ::).inner.toDenseMatrix
      val b = t._1.toDenseMatrix
      a * b
    })
    val shat = DenseMatrix.create(rows = si.collect().length, cols = si.collect().length, data = si.collect().flatMap(_.toArray))
    val yhat = getYHat(X, betas.collect())
    val residual = Y - yhat
    //    val s = calDiagnostic(X, Y, residual, shat)
    //    println(s)
    (betas.collect(), yhat, residual, shat, ci.collect())
  }

  // local GWR functions finish

}

object MGWR {

  def regress(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
              kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = false, iteration: Int = 20, epsilon: Double = 1e-5) = {
    val model = new MGWR(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
//    val re0=model.fitAll(kernel = kernel, approach = approach, adaptive = adaptive)
//    model.setKernel(kernel)
//    model.setApproach(approach)
//    model.setAdaptive(adaptive)
//    model.setEps(epsilon)
    model.setParams(kernel = kernel, approach = approach, adaptive = adaptive, eps = epsilon)
    val re = model.regress(iteration)

    Service.print(re._2, "Multiscale GWR", "String")
    sc.makeRDD(re._1)
  }

}