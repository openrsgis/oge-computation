package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.linalg.{DenseVector, _}
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{DoubleCellType, MultibandTile, RasterExtent, TileLayout, density}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.PointFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils.{createPredictionPoints, getExtent}
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal


object KernelDensityEstimation extends Algorithm {
  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dvecX: DenseVector[Double] = _
  private var _dmatX: DenseMatrix[Double] = _
  private var _nameX: String = _

  override def setX(property: String, split: String = ","): Unit = {
    _nameX = property
    _dvecX = DenseVector(_data.map(t => t(property).asInstanceOf[java.math.BigDecimal].doubleValue()).collect())
  }

  /**
   *
   * @param sc SparkContext
   * @param featureRDD RDD
   * @param propertyName Property to analyse
   * @param rows output rows
   * @param cols output cols
   * @param kernel type of kernel, including "gaussian" and "circle"
   * @param size size of kernel, must be odd
   * @param sigma sigma of gaussian kernel
   * @param amplitude amplitude of gaussian kernel
   * @param radius radius of circle kernel
   * @return raster
   */
  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          propertyName: Option[String] = None, rows: Int = 20, cols: Int = 20, kernel: String = "gaussian",
          size: Int = 3, sigma: Double = 1.0, amplitude: Double = 1.0, radius: Int = 2) ={
    val n = featureRDD.count()
    val extent = getExtent(featureRDD)
    val rasterExtent = RasterExtent(extent,cols,rows)
    val pointsRas = createPredictionPoints(extent, rows, cols)
    val cellWidth = (extent.xmax-extent.xmin)/cols
    val cellHeight = (extent.ymax-extent.ymin)/rows
    //println(f"$cellWidth, $cellHeight")

    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = if(propertyName.isEmpty == true) 1.0/n.toDouble else t._2._2(propertyName.get).asInstanceOf[java.math.BigDecimal].doubleValue()
      PointFeature(p, data)
    }).collect()

    if(size % 2 == 0) throw new IllegalArgumentException("Size of kernel must be odd.")

    val K = kernel match {
      case "gaussian" => Kernel.gaussian(size,sigma,amplitude)
      case "circle" => Kernel.circle(size, min(cellWidth,cellHeight),radius)
      case _ => throw new IllegalArgumentException("Error kernel type")
    }

    // calculation
    println("************************************************************")
    println(s"Parameters load correctly, start calculation")
    val densityTile = density.KernelDensity.apply(points.toTraversable, K,rasterExtent)
    val a = densityTile.toArrayDouble().toList

    // output
    val crs = OtherUtils.getCrs(featureRDD)

    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    //output raster value
    val featureRaster = pointsRas.zipWithIndex.map(t => {
      val data = a(t._2)
      val geom = t._1
      val feature = new vector.Feature[Geometry, Double](geom, data)
      //      println(geom,data)
      feature
    })
    //make rdd
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("kernel_density_estimation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    println("kernel density estimation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)
  }


//  /**
//   *
//   * @param sc SparkContext
//   * @param data RDD
//   * @param property 原始数据X的属性名
//   * @param bw 带宽，若为空，则默认根据输入数据优选带宽bw.nrd0
//   * @param n 估计密度时网格的个数，默认512
//   * @param cut 决定KDE网格的区间，cut默认为3
//   * @param kernel 核函数类型，默认为gaussian，此外还包括rectangular,triangular,epanechnikov,biweight,cosine及optcosine
//   * @param from 网格的起始，若为空，则默认为min(X)-cut*bw
//   * @param to 网格的终点，若为空，则默认为max(X)+cut*bw
//   * @return 字符串
//   */
//  def fit_legacy(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String,
//          bw: Option[Double] = None, n: Int = 512, cut: Int = 3, kernel: String = "gaussian",
//          from: Option[Double] = None, to: Option[Double] = None):String = {
//    _data = data.map(t => t._2._2)
//    setX(property)
//    val X = _dvecX
//
//    val bandwidth = bw.getOrElse(bw_nrd0(sc,X))
//    val nPoints = n
//    val minData = X.toArray.min
//    val maxData = X.toArray.max
//
//    // inspect
//    if (bandwidth <=0) {
//      throw new IllegalArgumentException("bw must be positive")
//    }
//    if(nPoints <= 1){
//      throw new IllegalArgumentException("number of grids is less than 2")
//    }
//
//    var gridMin = from.getOrElse(minData-cut*bandwidth)
//    var gridMax = to.getOrElse(maxData+cut*bandwidth)
//    // from > to时
//    if(gridMin > gridMax){
//      val a = gridMin
//      gridMin = gridMax
//      gridMax = a
//    }
//    val grid = breeze.linalg.linspace(gridMin, gridMax, nPoints)    // x of result
//    val x_rdd = DenseVectorToRDD(sc,grid)
//
//    // kde basic
//    val density = kde(X.toArray,grid.toArray,bandwidth,kernel)
//    val y_rdd = DenseVectorToRDD(sc,DenseVector(density))
//
//    val x_min = grid.toArray.min
//    val x_max = grid.toArray.max
//    val x_mean = mean(grid)
//    val x_1qu = computePercentile(x_rdd,25)
//    val x_median = computePercentile(x_rdd,50)
//    val x_3qu = computePercentile(x_rdd,75)
//
//    val y_min = density.min
//    val y_max = density.max
//    val y_mean = mean(density)
//    val y_1qu = computePercentile(y_rdd, 25)
//    val y_median = computePercentile(y_rdd, 50)
//    val y_3qu = computePercentile(y_rdd, 75)
//
//    // output string
//    val resMat = DenseMatrix.zeros[String](7,3)
//    resMat(0,0) = ""
//    resMat(0,1) = "x"
//    resMat(0,2) = "y"
//
//    resMat(1,0) = "Min."
//    resMat(2,0) = "1st Qu."
//    resMat(3,0) = "Median"
//    resMat(4,0) = "Mean"
//    resMat(5,0) = "3rd Qu."
//    resMat(6,0) = "Max."
//
//    resMat(1, 1) = x_min.toString
//    resMat(2, 1) = x_1qu.toString
//    resMat(3, 1) = x_median.toString
//    resMat(4, 1) = x_mean.toString
//    resMat(5, 1) = x_3qu.toString
//    resMat(6, 1) = x_max.toString
//
//    resMat(1, 2) = y_min.toString
//    resMat(2, 2) = y_1qu.toString
//    resMat(3, 2) = y_median.toString
//    resMat(4, 2) = y_mean.toString
//    resMat(5, 2) = y_3qu.toString
//    resMat(6, 2) = y_max.toString
//
//    val str = "\n**************Result of Kernel Density Estimation**************\n"+
//      f"Number of Samples: ${X.length}; Number of Grid: ${grid.length}\n"+
//      f"Kernel: $kernel; Bandwidth: ${bandwidth.formatted("%.4f")}\n"+
//      f"$resMat\n"+
//              "***************************************************************\n"
//    Service.print(str, "Kernel Density Estimation", "String")
//
//    str
//    //trigger modification required
//    }

//  protected def kernelSelection(name: String): Double => Double = {
//    name match {
//      case "gaussian"     => (u:Double) => (1.0 / sqrt(2 * Pi)) * exp(-0.5 * u * u)
//      case "rectangular"  => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.5
//      case "triangular"   => (u:Double) => if(scala.math.abs(u)>1) 0 else 1-scala.math.abs(u)
//      case "epanechnikov" => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.75*(1-u*u)
//      case "biweight"     => (u:Double) => if(scala.math.abs(u)>1) 0 else 15.0/16.0*(1 - u*u)*(1 - u*u)
//      case "cosine"       => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.5*(1+cos(Pi * u))
//      case "optcosine"    => (u:Double) => if(scala.math.abs(u)>1) 0 else Pi/4*cos(Pi/2*u)
//      case _ => throw new IllegalArgumentException("Unsupported Kernel")
//    }
//  }

//  protected def kde(data: Array[Double], grid: Array[Double], bandwidth: Double, kernel: String): Array[Double] = {
//    val n = data.length
//    val K = kernelSelection(kernel)
//    grid.map { point =>
//      data.map { xi =>
//        val u = (point - xi) / bandwidth
//        K(u)
//      }.sum / (n * bandwidth)
//    }
//  }

//  protected def bw_nrd0(sc: SparkContext,X:DenseVector[Double]):Double={
//    if(X.length < 2) throw new IllegalArgumentException("require at least 2 samples")
//    val stddev = breeze.stats.stddev(X)
//    var lo = math.min(stddev,computePercentile(DenseVectorToRDD(sc,X),75)/1.34)
//    if(lo == 0) {
//      lo = stddev
//      if(lo == 0){
//        lo = math.abs(X(1))
//        if(lo == 0) {
//          lo = 1
//        }
//      }
//    }
//    0.9 * lo * math.pow(X.length,-0.2)
//  }

  // bw with coordinates (based on standard distance)
//  protected def bw_nrd0(sc: SparkContext, X: DenseVector[Double], Y: DenseVector[Double], W: DenseVector[Double]): Double = {
//    val n = X.length
//    if (X.length < 2) throw new IllegalArgumentException("require at least 2 samples")
//    if (X.length != Y.length) throw new IllegalArgumentException("number of coordinate X is not equal to Y")
//    // Uniformed weight
//    val w_uni = W.map{_/W.sum}
//
//    // （加权）平均中心
//    val weightedCenterX = (0 until X.length).map(t => X(t)*w_uni(t)).sum
//    val weightedCenterY = (0 until Y.length).map(t => Y(t)*w_uni(t)).sum
//
//    //println(f"CenterX: $weightedCenterX\nCenterY: $weightedCenterY")
//
//    // 与平均中心的距离
//    val distances = (0 until X.length).map{ i =>
//      val dx = X(i) - weightedCenterX
//      val dy = Y(i) - weightedCenterY
//      sqrt(dx * dx + dy * dy)
//    }.toArray
//
//    // 距离的中值
//    val medianDistance = computePercentile(DenseVectorToRDD(sc,DenseVector(distances)),50)
//
//    //加权标准距离
//    val sd = sqrt((0 until X.length).map{ i =>
//      val dx = X(i) - weightedCenterX
//      val dy = Y(i) - weightedCenterY
//      w_uni(i)*(dx*dx+dy*dy) // 加权平方差
//    }.sum)
//
//    0.9*min(sd,medianDistance/sqrt(scala.math.log(2)))*pow(n,-0.2)
//
//  }

  /**
   * compute percentile from an unsorted Spark RDD
   *
   * @param data : input data set of Long integers
   * @param tile : percentile to compute, [0,100]
   * @return value of input data at the specified percentile
   */
  protected def computePercentile(data: RDD[Double], tile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val percentile = if(tile <0) 0 else if (tile >100) 100 else tile
    val sortedData = data.sortBy(x => x)
    val count = sortedData.count()

    if (count == 0) {
      throw new IllegalArgumentException("Data RDD is empty")
    }

    val index = percentile/100.0 * (count -1)
    val lowerIndex = math.floor(index).toLong
    val upperIndex = math.ceil(index).toLong

    val indices = sortedData.zipWithIndex().map(_.swap)
    if (lowerIndex == upperIndex) {
      indices.lookup(lowerIndex).head
    } else {
      val lowerValue = indices.lookup(lowerIndex).head
      val upperValue = indices.lookup(upperIndex).head
      lowerValue + (upperValue - lowerValue) * (index - lowerIndex)
    }
  }

  protected def DenseVectorToRDD(sc: SparkContext, data:DenseVector[Double]):RDD[Double]={
    sc.parallelize(data.toArray)
  }

  protected def formatToSignificantFigures(value: Double, significantFigures: Int): String = {
    val precision =  BigDecimal(value).precision
    val scale = significantFigures - precision + 1
    println(scale)
    BigDecimal(value).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toString
  }

}
