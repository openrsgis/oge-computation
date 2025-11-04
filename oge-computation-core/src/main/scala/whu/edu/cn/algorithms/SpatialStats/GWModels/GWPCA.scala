package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, cholesky, diag, eigSym, inv, svd}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize.goldenSelection
import whu.edu.cn.oge.Service

import scala.collection.mutable

//import scala.actors.migration.ActWithStash.continue
import scala.math._
import scala.util.Random

class GWPCA(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD){

  val select_eps = 1e-2
  var _nameUsed: Array[String] = _
  var _outString: String = _

  private val epsilon = 1e-6 // 正则化常数，确保矩阵可逆

  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  private var _k: Int = 2
  private var _robust: Boolean = false
  protected def setX_gwpca(properties: String)={
    setX(properties)
    _dmatX = _dmatX(::, 1 until _cols)
    _dvecX = _dvecX.drop(1)
    _cols = _cols - 1
  }

  protected def setParams(kernel: String, adaptive: Boolean, k: Int, robust: Boolean) ={
    _kernel = kernel
    _adaptive = adaptive
    _k = k
    _robust = robust
  }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "CV", adaptive: Boolean = true): Double = {
    println("Selecting bandwidth...")
    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "CV", upper: Double = _disMax, lower: Double = _disMax / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    var approachfunc: Double => Double = bandwidthCV
    //    if (approach == "CV") {
    //      approachfunc = bandwidthCV
    //    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1
    } catch {
      case e: Exception => {
        println("meet matrix singular error")
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "CV", upper: Int = _rows - 1, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    var approachfunc: Double => Double = bandwidthCV
    //    if (approach == "CV") {
    //      approachfunc = bandwidthCV
    //    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1.toInt
    } catch {
      case e: Exception => {
        println("meet matrix singular error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def bandwidthCV(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
//    newWeight.take(1).foreach(println)
    val cvWeight = modifyWeightCV(newWeight)
    val results = gwpca_fit(weight = cvWeight, k = _k)
    val loading = results._5
    val vvts = loading.map(u => u * u.t)
    val cvScore = (0 until _rows).map{i =>{
      val x = _dmatX(i,::).t.toDenseMatrix
      val vvt = vvts(i)
      val digit = x - x * vvt
      pow(digit.sum,2)
    }}.sum
    if(_adaptive)
    {println(f"Bandwidth: ${round(bw)}, CV score: $cvScore")
    }else{
      println(f"Bandwidth: $bw, CV score: $cvScore")
    }
    cvScore
  }

  private def modifyWeightCV(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.zipWithIndex.map { case (t, idx) =>
      val modifiedVector = t.copy
      modifiedVector(idx.toInt) = 0 // 只将当前点的权重设为0
      modifiedVector
    }
  }

  def gwpca_fit(X: DenseMatrix[Double] = _dmatX, weight: RDD[DenseVector[Double]] = _spWeight, k: Int)
   = {
    // 矩阵分解，Sigma = UDV，其实结果只有DV
    val UDV = if(_robust) weight.map { w_i => weightedPCA_robust(_dmatX, w_i, k)} else
      weight.map { w_i => weightedPCA(_dmatX, w_i, k)}
    val D = UDV.map(_._1)
    val V = UDV.map(_._2)
    // loading
    val loading = (0 until k).map(i=>{
      val loadings = V.map(v=>v(::,i)).collect()
      //先按列填充数据再转置
      DenseMatrix.create(rows = _cols, cols = _rows, data = loadings.flatMap(_.toArray)).t
    }).toArray

    val gwpca_scores = V.map(v => X * v).collect()

    // variance
//    val sum_wt = weight.collect().last.sum
    val variance = if(_robust){
      D.zip(weight).map { case (d, wt) =>
        d.map(v => math.pow(v, 2))
      }
    }else {
      D.zip(weight).map { case (d, wt) =>
        val sumWt = wt.sum
        d.map(v => math.pow(v / math.sqrt(sumWt), 2)) //r代码这个地方有问题，换成每一行权之和sumWt
      }
    }

    val localPV = variance.map { d1vec =>
      val total = d1vec.sum
      // 取前k个主成分
      val pv = d1vec(0 until k).map(_ / total * 100.0)
      pv
    }

    (loading, gwpca_scores, variance.collect(), localPV.collect(), V.collect())

  }

  // 正则化函数，向矩阵的对角线添加小常数 epsilon
  def regularizeMatrix(matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val eye = DenseMatrix.eye[Double](matrix.rows)
    matrix + eye * epsilon //添加epsilon
  }

  /**
   * 对全体样本做加权PCA，返回SVD结果
   *
   * @param X DenseMatrix(n, m) 所有样本的自变量
   * @param w DenseVector(n) 当前样本的空间权重
   * @return (特征值, 特征向量)
   */
  protected def weightedPCA(X: DenseMatrix[Double], w: DenseVector[Double], k: Int): (DenseVector[Double], DenseMatrix[Double]) = {
    val n = X.rows
    val m = X.cols
    val sumW = w.sum

    val validIdx = (0 until n).filter(i => w(i) > 1e-8)
    if (validIdx.length <= 5) {
      // 返回全零或NaN，或者抛出异常，由上层处理
      return (DenseVector.zeros[Double](k), DenseMatrix.zeros[Double](m, k))
    }
    // ...后续只对validIdx做PCA...

    // 1. 计算加权均值
    val weightedMeans = DenseVector.zeros[Double](m)
    for (j <- 0 until m) {
      weightedMeans(j) = (0 until n).map(i => X(i, j) * w(i)).sum / sumW
    }

    // 2. 加权中心化
    val X_centered = DenseMatrix.zeros[Double](n, m)
    for (i <- 0 until n; j <- 0 until m) {
      X_centered(i, j) = X(i, j) - weightedMeans(j)
    }

    // 3. 每行乘以sqrt(w_i)
    val X_weighted = DenseMatrix.zeros[Double](n, m)
    for (i <- 0 until n; j <- 0 until m) {
      X_weighted(i, j) = X_centered(i, j) * math.sqrt(w(i))
    }

    // 4. SVD
    val svd.SVD(u, s, vt) = svd(X_weighted)
    // s: 特征值，vt.t: 特征向量（每列为一个主成分方向）
    (s, vt.t(::, 0 until k))
  }




  def fit(bw: Double, k: Int, adaptive: Boolean, kernel: String)={
    _kernel = kernel
    _adaptive = adaptive
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val res = gwpca_fit(k = k, weight = newWeight)

    val loading =  res._1
    val score = res._2
    val variance = res._3
    val localPV = res._4

//    println(f"Loading: \n${loading(0)}")
//    println(f"score: \n${score(0)}")
//    println(f"variances: \n${variance(0)}")
//    println(f"LocalPV: \n${localPV(0)}")

    val shpRDDidx = _shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    shpRDDidx.foreach(t => {
      for(i <- 0 until _cols){
        for(j <- 0 until k){
          t._1._2._2 += (_nameX(i)+"_PC"+(j+1).toString -> loading(j)(::,i))
        }
      }
    })

    if (_nameUsed == null) {
      _nameUsed = _nameX
    }
    var bw_type = "Fixed"
    if (adaptive) {
      bw_type = "Adaptive"
    }
    val bw_output = if(adaptive) round(bw).toInt.toString else bw.formatted("%.2f")
    val fitVariables = _nameUsed.mkString(" ")
    var fitString = "\n*********************************************************************************\n" +
      "*        Results of Geographically Weighted Principal Components Analysis       *\n" +
      "*********************************************************************************\n" +
      "**************************Model calibration information**************************\n" +
      s"Variables: $fitVariables" +
      s"\nKernel function: $kernel\n$bw_type bandwidth: " + f"$bw_output" +
      f"\nLocal variance:\n${summaryLocalVar(variance, k)}" +
      f"\nLocal Proportion of Variance:\n${summaryLocalPropVar(localPV,k)}"

    // println(fitString)
    (shpRDDidx.map(t => t._1), fitString)
  }

  protected def summaryLocalVar(variance: Array[DenseVector[Double]],k:Int)={
    val colNames = DenseVector(Array("","Min.", "1st Qu.", "Median", "3rd Qu.", "Max."))
//    val rowNames = DenseVector(Array("") ++ (1 to k).map{ i => "Comp. "+ i.toString }.toArray)
    val arr = Array("") ++ (1 to k).map(i => "Comp. " + i.toString)
    val rowNames = DenseVector(arr: _*)
    val summaryMatrix = DenseMatrix.zeros[String](rowNames.length,colNames.length)
    summaryMatrix(0 to 0,::) := colNames
    summaryMatrix(::,0 to 0) := rowNames
    for(i<- 0 until k){
      val summary0 = summary(variance.map(t => t(i)))
      summaryMatrix(i+1 to i+1,1 until summaryMatrix.cols) := summary0.map(_.formatted("%.2f"))
    }
    summaryMatrix
  }

  protected def summaryLocalPropVar(localPV: Array[DenseVector[Double]], k: Int) = {
    val colNames = DenseVector(Array("", "Min.", "1st Qu.", "Median", "3rd Qu.", "Max."))
    //    val rowNames = DenseVector(Array("") ++ (1 to k).map{ i => "Comp. "+ i.toString }.toArray)
    val arr = Array("") ++ (1 to k).map(i => "Comp. " + i.toString) ++ Array("Cumulative")
    val rowNames = DenseVector(arr: _*)
    val summaryMatrix = DenseMatrix.zeros[String](rowNames.length, colNames.length)
    summaryMatrix(0 to 0, ::) := colNames
    summaryMatrix(::, 0 to 0) := rowNames
    var cumulative = DenseVector.zeros[Double](colNames.length-1)
    for (i <- 0 until k) {
      val summary0 = summary(localPV.map(t => t(i)))
      summaryMatrix(i + 1 to i + 1, 1 until summaryMatrix.cols) := summary0.map(_.formatted("%.2f"))
      cumulative = cumulative + summary0
    }
    val cumulStr = cumulative.map(_.formatted("%.2f"))
//    println(cumulStr)
    summaryMatrix(summaryMatrix.rows-1 to summaryMatrix.rows-1,1 to summaryMatrix.cols-1) := cumulStr
    summaryMatrix
  }

  protected def summary(arr: Array[Double]) = {
    val arrMin = arr.min
    val arrMax = arr.max
    val sorted = arr.sorted
    val q1 = getPercentTile(sorted, 0.25)
    val q2 = getPercentTile(sorted, 0.5)
    val q3 = getPercentTile(sorted, 0.75)
    val res = Array(arrMin, q1, q2, q3, arrMax)
    DenseVector(res)
  }
  protected def getPercentTile(arr: Array[Double], percent: Double) = {
    val sorted = arr.sorted
    val length = arr.length.toDouble
    val idx = (length + 1.0) * percent
    val i = idx.floor.toInt
    val j = idx - i.toDouble
    sorted(i - 1) * j + sorted(i) * (1 - j)

  }

  //below is codes about robust gwpca
  def mcdCov(X: DenseMatrix[Double], h: Int, nsamp: Int = 500): (DenseVector[Double], DenseMatrix[Double], DenseVector[Double]) = {
//    println(f"h: $h")
    val n = X.rows
    val p = X.cols

    if (n <= p) throw new IllegalArgumentException("n <= p: too few samples for MCD")
    if (n < 2 * p) println("Warning: n < 2p, possibly too small sample size")
    if (h > n) throw new IllegalArgumentException("Sample size n < h(alpha; n,p)")

    var minDet = Double.MaxValue
    var bestMean: DenseVector[Double] = null
    var bestCov:  DenseMatrix[Double] = null
    var bestIdx: Seq[Int] = Seq()


    def detSymmetric(mat: DenseMatrix[Double]): Double = {
      val chol = cholesky(mat)
      val diagProd = diag(chol).data.product
      diagProd * diagProd
    }

    val rand = new Random()
    for (_ <- 0 until nsamp) {
      val idx = rand.shuffle((0 until n).toList).take(h)
      val subset = X(idx, ::).toDenseMatrix

      val mean = breeze.stats.mean(subset(::, *)).inner
      val centered = subset(*, ::) - mean

      val colVars = (0 until p).map(j => breeze.stats.variance(subset(::, j)))
      if (colVars.exists(_ < 0)) {
//        println(s"Warning: colVars too small")
        // 跳过
      } else {
        val cov = (centered.t * centered) / (h - 1).toDouble + DenseMatrix.eye[Double](p) * 1e-8
        val det = detSymmetric(cov)
        if (detSymmetric(cov) <= 0) {
//          println(s"Warning: det<=0")
          // 跳过
        } else {
          if (det < minDet && det > 0) {
            minDet = det
            bestMean = mean
            bestCov = cov
            bestIdx = idx
          }
        }
      }
    }

    if (bestCov == null) {
      // 可以返回单位阵或全零阵，或者抛出异常
//      throw new IllegalArgumentException("MCD failed: no non-singular subset found")
//      // 返回单位阵和全零均值
//      val mean = DenseVector.zeros[Double](p)
//      val cov = DenseMatrix.eye[Double](p)
//      val mahal = DenseVector.zeros[Double](n)
//      return(mean, cov, mahal)
      // 采样全失败，降级为经典协方差
      val mean = breeze.stats.mean(X(::, *)).inner
      val centered = X(*, ::) - mean
      val cov = (centered.t * centered) / (n - 1).toDouble
      val invCov = inv(cov + DenseMatrix.eye[Double](p) * 1e-8) // 加一点正则化
      val mahal = (0 until n).map { i =>
        val diff = X(i, ::).t - mean
        math.sqrt((diff.t * invCov * diff))
      }
      return (mean, cov, DenseVector(mahal.toArray))
    }

    // 计算所有点的马氏距离
    val invCov = inv(bestCov)
    val mahal = (0 until n).map { i =>
      val diff = X(i, ::).t - bestMean
      math.sqrt((diff.t * invCov * diff))
    }
    (bestMean, bestCov, DenseVector(mahal.toArray))
  }

  def reweightedCov(X: DenseMatrix[Double], mean: DenseVector[Double], cov: DenseMatrix[Double], mahal: DenseVector[Double], threshold: Double): (DenseVector[Double], DenseMatrix[Double], DenseVector[Double]) = {
    val n = X.rows
    val weights = DenseVector(mahal.data.map(d => if (d <= threshold) 1.0 else 0.0))
    val sumW = weights.sum
    val weightedX = (0 until n).map(i => X(i, ::).t * weights(i)).reduce(_ + _)
    val weightedMean = weightedX / sumW
    val centered = X(*, ::) - weightedMean
    val weightedCov = (centered.t * (centered(::, *) * weights)) / sumW
    (weightedMean, weightedCov, weights)
  }

  def robustSVD(X: DenseMatrix[Double]) = {
    val h = (X.rows * 0.75).toInt
    val (robustMean, robustCov, mahal) = mcdCov(X, h)
    val eigSym.EigSym(eigenvalues, eigenvectors) = eigSym(robustCov)
    val sdev = eigenvalues.map(math.sqrt)

    val X_centered = X(*, ::) - robustMean
    val u = X_centered * eigenvectors
    val vt = eigenvectors.t

    (u, sdev, vt)
  }

  // 对每一列计算加权中位数
  def weightedMedian(X: DenseMatrix[Double], w: DenseVector[Double]): DenseVector[Double] = {

    def wt_median1(x: DenseVector[Double], w: DenseVector[Double]): Double = {
      // 排序
      //    println(x)
      val idx = (0 until x.length).toArray
      val sortedIdx = idx.sortBy(x(_)) // 按x排序后的索引
      val xSorted = sortedIdx.map(x(_))
      val wSorted = sortedIdx.map(w(_))
      val wCumSum = wSorted.scanLeft(0.0)(_ + _).tail
      val totalW = w.sum
      val halfW = totalW / 2.0
//      val pos = wCumSum.indexWhere(_ >= halfW)
      val pos = wCumSum.zipWithIndex.minBy { case (cum, _) => math.abs(cum - 0.5) }._2//这里后面改成 cum - halfW
      if (pos < 0 || pos >= xSorted.length) {
        // 兜底：返回中位数或均值或0，或抛出更友好的异常
        return xSorted(xSorted.length / 2)
      }
      xSorted(pos)
    }

    DenseVector((0 until X.cols).map(j => wt_median1(X(::, j), w)).toArray)
  }

  def weightedPCA_robust(X: DenseMatrix[Double], w: DenseVector[Double], k: Int): (DenseVector[Double], DenseMatrix[Double]) = {
    val n = X.rows
    val m = X.cols

    val validIdx = (0 until n).filter(i => w(i) > 1e-8)
    if (validIdx.length <= 5) {
      // 返回全零或NaN，或者抛出异常，由上层处理
      return (DenseVector.zeros[Double](k), DenseMatrix.zeros[Double](m, k))
    }
    // ...后续只对validIdx做PCA...

    // 1. 计算每列的加权中位数
    val medians = weightedMedian(X, w)
    //    println(f"median: $medians")

    // 2. 中位数中心化
    val X_centered = DenseMatrix.zeros[Double](n, m)
    for (i <- 0 until n; j <- 0 until m) {
      X_centered(i, j) = X(i, j) - medians(j)
    }

    // 3. 每行乘以 sqrt(w_i)
    val X_weighted = DenseMatrix.zeros[Double](n, m)
    for (i <- 0 until n; j <- 0 until m) {
      X_weighted(i, j) = X_centered(i, j) * math.sqrt(w(i))
    }

    // 4. SVD
    val (u, s, vt) = robustSVD(X_weighted)
    // s: 特征值，vt.t: 特征向量（每列为一个主成分方向）
    (s(0 until k), vt(::, 0 until k))
  }



}

object GWPCA {
  /**
   *
   * @param sc SparkContext
   * @param featureRDD 输入数据
   * @param properties 待分析的自变量
   * @param bandwidth 带宽，若小于等于0则自动优选，否则按指定带宽计算，默认为"-1"
   * @param kernel 核函数，默认为"gaussian"
   * @param adaptive 带宽类型，Boolean类型，默认为true（适应性带宽）
   * @param k 保留的主成分数，默认为2
   * @return 包含各自变量在各主成分上载荷的矢量数据
   */
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], properties: String,
          bandwidth: Double = -1, kernel: String = "gaussian", adaptive: Boolean = true, k: Int = 2):
  RDD[(String, (Geometry, mutable.Map[String, Any]))] ={
    val model = new GWPCA(featureRDD)
    model.setX_gwpca(properties)
    model.setParams(kernel, adaptive, k = k, robust = false) // robust gwpca有问题

    val bw = if(bandwidth <= 0) model.bandwidthSelection(kernel, adaptive = adaptive) else bandwidth
    val re = model.fit(bw = bw, k = k, adaptive, kernel)
    Service.print(re._2, "GWPCA calculation", "String")
    sc.makeRDD(re._1)
  }
}
