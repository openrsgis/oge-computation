package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleCellType, MultibandTile, TileLayout}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.PointFeature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NearestNeighbourInterpolation {

  /**
   *
   * @param sc SparkContext
   * @param featureRDD RDD
   * @param propertyName property name, string
   * @param rows rows of interpolated raster, default 20
   * @param cols columns of interpolates raster, default 20
   * @return RDD[(SpaceTimeBandKey, MultibandTile)]
   */
  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          propertyName: String, rows: Int = 20, cols: Int = 20):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
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

    // interpolation
    val interpolatedValues = pointsRas.map { ptRas =>
      val nearest = points.minBy(pt => ptRas.distance(pt.geom)) // find the nearest point
      nearest.data // data of nearest point
    }
    //println(interpolatedPoints.toList)

    //output
    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    //output raster value
    val featureRaster = makeRasterVarOutput(pointsRas, interpolatedValues)
    //make rdd
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("nearest_neighbour_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    println("nearest neighbour interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)


  }



}
