package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster._
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.PointFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object IDW {

  /**
   *
   * @param sc           SparkContext
   * @param featureRDD   RDD
   * @param propertyName property name
   * @param rows         rows of interpolated raster
   * @param cols         cols of interpolated raster
   * @param radiusX      search radius in X-axis direction, the unit of which is the same as that of RDD.
   * @param radiusY      search radius in Y-axis direction, the unit of which is the same as that of RDD.
   * @param rotation     rotation degree of search area
   * @param weightingPower higher values result in faster decay of weight
   * @param smoothingFactor higher values produce smoother results
   * @param equalWeightRadius radius within which all points have equal weight
   * @return raster
   */
  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          propertyName: String, rows: Int = 20, cols: Int = 20,
          radiusX: Double = 1, radiusY: Double = 1, rotation: Double = 0,
          weightingPower: Double = 2, smoothingFactor: Double = 0, equalWeightRadius: Double = 0)
  ={
    val extent = getExtent(featureRDD)
    val crs = OtherUtils.getCrs(featureRDD)
    val pointsRas = createPredictionPoints(extent, rows, cols)

    //convert points
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[java.math.BigDecimal].doubleValue()
      PointFeature(p, data)
    }).collect()

    val rasterExtent = RasterExtent(extent,cols,rows)

    println("************************************************************")
    println(s"Parameters load correctly, start calculation")

    val idw = geotrellis.raster.interpolation.InverseDistanceWeighted
    //val cellType = geotrellis.raster.CellType.fromName("")
    val option = idw.Options.apply(
      radiusX, radiusY, rotation, weightingPower, smoothingFactor, equalWeightRadius, DoubleConstantNoDataCellType
    )
    val tile = idw.apply(points.toTraversable,rasterExtent,option)
    val interpolatedValues = tile.tile.toArrayDouble()

    //println(interpolatedValue.toList)

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
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("IDW_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    println("IDW interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)
  }

}
