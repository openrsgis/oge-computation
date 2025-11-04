package whu.edu.cn.util

import geotrellis.proj4.CRS

import scala.collection.mutable.Map

object CRSUtils {
  val crsMap: Map[String, CRS] = Map.empty[String, CRS]


  def getCrsByName(name: String): CRS = {
    if (crsMap.contains(name)) {
      crsMap(name)
    } else {
      crsMap.put(name, CRS.fromName(name))
      crsMap(name)
    }
  }
}
