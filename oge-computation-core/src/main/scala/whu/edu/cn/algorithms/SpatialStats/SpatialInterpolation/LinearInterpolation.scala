package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleCellType, MultibandTile, TileLayout}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.{PointFeature, withDelaunayTriangulationArrayMethods}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LinearInterpolation {

  /**
   *
   * @param sc SparkContext
   * @param featureRDD RDD
   * @param propertyName property name
   * @param rows rows of interpolated raster
   * @param cols cols of interpolated raster
   * @return raster
   */
  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          propertyName: String, rows: Int = 20, cols: Int = 20):
  (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) ={
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

    val coordinates = points.map(_.getCoordinate)
    val triangulation = coordinates.delaunayTriangulation
    val interpolatedValues = new Array[Double](rows * cols)

    //println(triangulation.triangleMap.getTriangles())

    for (i <- 0 until rows * cols){
      val predictPoint = pointsRas(i)
      val predictPointCoordinate = predictPoint.getCoordinate

      // Delaunay triangles
      val triangles = triangulation.triangleMap.getTriangles().map(t => {
        val pt1 = points(t._1._1).getCoordinate
        val pt2 = points(t._1._2).getCoordinate
        val pt3 = points(t._1._3).getCoordinate
        val data = (points(t._1._1).data,points(t._1._2).data,points(t._1._3).data)
        (vector.Polygon(Seq(pt1,pt2,pt3,pt1)),data)
      }).toVector

      val containingTriangle = triangles.find(_._1.contains(predictPoint))

      if(containingTriangle.isDefined){
        val containingTriangleCoordinates = containingTriangle.map(_._1).get
        val containingTriangleData = containingTriangle.map(_._2).get
        val centroid = getDelaunayCentroid(containingTriangleCoordinates,predictPointCoordinate)
        //println(centroid)

        // interpolation
        val z = containingTriangleData._1 * centroid._1 +
          containingTriangleData._2 * centroid._2 +
          containingTriangleData._3 * centroid._3

        interpolatedValues(i) = z
      }else{
        // nearest neighbour interpolation
        interpolatedValues(i) = points.minBy(pt => predictPoint.distance(pt.geom)).data
      }
    }

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

    println("linear interpolation succeeded")
    println("************************************************************")

    (imageRDD, tileLayerMetadata)

  }

  private def getDelaunayCentroid(triangle: vector.Polygon, coordinate: Coordinate) = {
//    val xs = triangle.getCoordinates.map(_.x)
//    val ys = triangle.getCoordinates.map(_.y)
    val pt = triangle.getCoordinates

//    val xa = xs(0)
//    val xb = xs(1)
//    val xc = xs(2)
//    val ya = ys(0)
//    val yb = ys(1)
//    val yc = ys(2)

    val area = triangle.getArea

    val subtriangle0 = vector.Polygon(Seq(coordinate,pt(1),pt(2),coordinate))
    val subtriangle1 = vector.Polygon(Seq(coordinate,pt(0),pt(2),coordinate))
    val subtriangle2 = vector.Polygon(Seq(coordinate,pt(0),pt(1),coordinate))

    val alpha = subtriangle0.getArea/area
    val beta = subtriangle1.getArea/area
    val gamma = subtriangle2.getArea/area

    (alpha, beta, gamma)
  }

}
