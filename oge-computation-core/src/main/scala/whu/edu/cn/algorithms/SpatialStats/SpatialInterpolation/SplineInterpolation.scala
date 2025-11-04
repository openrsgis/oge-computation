package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.pow
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleCellType, MultibandTile, TileLayout}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.PointFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Geometry, Point}
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.{log, min}

object SplineInterpolation {

  /** thin plate spline interpolation
   *
   * @param sc SparkContext
   * @param featureRDD RDD
   * @param propertyName property name
   * @param rows rows of output raster
   * @param cols columns of output raster
   * @return output raster
   */
  def thinPlateSpline(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
                      propertyName: String, rows: Int = 20, cols: Int = 20)={
    val extent = getExtent(featureRDD)
    val crs = OtherUtils.getCrs(featureRDD)
    val pointsRas = createPredictionPoints(extent, rows, cols)
    //convert points
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[java.math.BigDecimal].doubleValue()
      PointFeature(p, data)
    }).collect()

    println("************************************************************")
    println(s"Parameters load correctly, start calculation")

    val interpolatedValues = TPS(points,pointsRas)

    // output
    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    // output raster value
    val featureRaster = makeRasterVarOutput(pointsRas, interpolatedValues)
    //make rdd
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("Thin_plate_spline_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })

    println("thin plate spline interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)
  }

  /** B spline interpolation
   *
   * @param sc           SparkContext
   * @param featureRDD   RDD
   * @param propertyName property name
   * @param rows         rows of output raster
   * @param cols         columns of output raster
   * @return output raster
   */
  def BSpline(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
              propertyName: String, rows: Int = 20, cols: Int = 20)={
    val extent = getExtent(featureRDD)
    val crs = OtherUtils.getCrs(featureRDD)
    val pointsRas = createPredictionPoints(extent, rows, cols)
    //convert points
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[java.math.BigDecimal].doubleValue()
      PointFeature(p, data)
    }).collect()

    println("************************************************************")
    println(s"Parameters load correctly, start calculation")

    val m = cols // x
    val n = rows // y
    // determine control lattice PHI
    val PHI = Array.ofDim[Double](m + 3, n + 3)
    // range of data point
    val xCoords = points.map(_.getX)
    val yCoords = points.map(_.getY)
    val xMin = xCoords.min
    val xMax = xCoords.max
    val yMin = yCoords.min
    val yMax = yCoords.max


    // form a formula group
    val numPoints = points.length
    val numControlPoints = (m+3)*(n+3)
    val mat_A = DenseMatrix.zeros[Double](numPoints,numControlPoints)
    val mat_b = DenseVector.zeros[Double](numPoints)

    for(p <- points.indices){
      val point = points(p)
      // normalized coordinate
      val u = (point.getX - xMin) / (xMax - xMin) * m
      val v = (point.getY - yMin) / (yMax - yMin) * n
      val i = min(math.floor(u).toInt,m-1)
      val j = min(math.floor(v).toInt,n-1)
      val s = u - i
      val t = v - j
      // coefficients matrix
      for(k<-0 to 3;l <-0 to 3){
        val bk = BSplineBasic(s, k)
        val bl = BSplineBasic(t, l)
        val colIndex = (i+k)*(n+3) +(j+l)
        mat_A(p,colIndex) = bk * bl
//        print(f"$colIndex ")
      }
      mat_b(p) = point.data
    }



    // solve formula group (ols)
    val lambda = 0.1
    val I = DenseMatrix.eye[Double](numControlPoints)
    val PHI_solved = breeze.linalg.inv(mat_A.t * mat_A + lambda * I) * mat_A.t * mat_b
    // solved -> lattice
    for(i <- 0 until m+3;j <- 0 until n+3){
      PHI(i)(j) = PHI_solved((i)*(n+3)+(j))
    }

//    println(PHI.map(t => println(t.toList)))

    // interpolation
    val interpolatedValues = pointsRas.map{ point =>
      // normalization
      val u = (point.getX - xMin)/(xMax - xMin) * m
      val v = (point.getY - yMin)/(yMax - yMin) * n
      // find the control point
      val i = math.floor(u).toInt
      val j = math.floor(v).toInt
      // local coordinate
      val s = u - i
      val t = v - j
      // B Spline basic function to interpolate
      var result = 0.0
      for(k<- 0 to 3; l <- 0 to 3){
        val bk = BSplineBasic(s,k)
        val bl = BSplineBasic(t,l)
        result += PHI(i+k)(j+l) * bk * bl
      }
      result
    }

    // output
    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    // output raster value
    val featureRaster = makeRasterVarOutput(pointsRas, interpolatedValues)
    //make rdd
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("B_spline_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })

    println("B spline interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)

  }

  private def TPS(points:Array[PointFeature[Double]],pointsRas:Array[Point])={
    val n = points.length

    val mat_A = DenseMatrix.zeros[Double](n + 3, n + 3) // [PHI P, P.t 0]
    val mat_b = DenseMatrix.zeros[Double](n + 3, 1) // [z,0]

    val phi = (r: Double) => if (r > 0) pow(r, 2) * log(r) else 0 //kernel function, r is distance
    for (i <- 0 until n; j <- 0 until n) {
      val p_i = points(i).geom
      val p_j = points(j).geom
      val dist = p_i.distance(p_j)
      mat_A(i, j) = phi(dist)
    } // distances
    for (i <- 0 until n) {
      val p_i = points(i)
      // coordinates
      mat_A(i, n) = 1
      mat_A(i, n + 1) = p_i.getX
      mat_A(i, n + 2) = p_i.getY
      mat_A(n, i) = mat_A(i, n)
      mat_A(n + 1, i) = mat_A(i, n + 1)
      mat_A(n + 2, i) = mat_A(i, n + 2)
      // attribute values
      mat_b(i, 0) = p_i.data
    }

    val mat_a = breeze.linalg.inv(mat_A) * mat_b
    val w = mat_a(0 until n, 0)
    val coefficients = mat_a(n until n + 3, 0)

    val interpolatedValues = pointsRas.map(t => {
      val vec1 = DenseVector(1.0, t.getX, t.getY)
      val T = (0 until 3).map(t => {
        vec1(t) * coefficients(t)
      }).sum

      // sum(w*phi)
      val w_phi = (0 until n).map(i => {
        val dist = points(i).distance(t)
        val phi_dist = phi(dist)
        phi_dist * w(i)
      }).sum

      T + w_phi
    })
    interpolatedValues
  }

  private def BSplineBasic(t:Double, p:Int)={
    p match {
      case 0 =>     pow(1-t,3)                        /6
      case 1 => ( 3*pow(  t,3) - 6*pow(t,2)       + 4)/6
      case 2 => (-3*pow(  t,3) + 3*pow(t,2) + 3*t + 1)/6
      case 3 =>     pow(  t,3)                        /6
    }
  }
}
