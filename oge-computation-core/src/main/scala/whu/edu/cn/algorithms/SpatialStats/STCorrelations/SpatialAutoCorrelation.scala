package whu.edu.cn.algorithms.SpatialStats.STCorrelations

import breeze.linalg._
import breeze.plot._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.{pow, sqrt}

object SpatialAutoCorrelation {

  /**
   * 输入RDD直接计算全局莫兰指数
   *
   * @param featureRDD     RDD
   * @param property    要计算的属性，String
   * @param plot        bool类型，是否画散点图，默认为否(false)
   * @param test        是否进行测试(计算P值等)
   * @param weightstyle 邻接矩阵的权重类型，参考 getNeighborWeight 函数
   * @return 全局莫兰指数，峰度
   */
  def globalMoranI(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, plot: Boolean = false, test: Boolean = false, weightstyle: String = "W"): String = {
    val nb_weight = getNeighborWeight(featureRDD, weightstyle)
    val sum_weight = sumWeight(nb_weight)
    val input = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue())
    val arr_mean = rddmeandiff(input)
    val meanidx = arr_mean.collect()
    val n = meanidx.length
    val arr_mul = arr_mean.map(t => {
      val re = new Array[Double](n)
      for (i <- 0 until n) {
        re(i) = t * meanidx(i)
      }
      DenseVector(re)
    })
    val weight_m_arr = rrdvec2multi(nb_weight, arr_mul.collect())
    val rightup = weight_m_arr.map(t => sum(t)).sum
    val rightdn = arr_mean.map(t => t * t).sum
    val moran_i = n / sum_weight * rightup / rightdn
    val kurtosis = (n * arr_mean.map(t => pow(t, 4)).sum) / pow(rightdn, 2)
    var outStr = s"global Moran's I is: ${moran_i.formatted("%.4f")}\n"
    outStr += s"kurtosis is: ${kurtosis.formatted("%.4f")}\n"
    if (test) {
      val E_I = -1.0 / (n - 1)
      val S_1 = 0.5 * nb_weight.map(t => sum(t * 2.0 * t * 2.0)).sum()
      val S_2 = nb_weight.map(t => sum(t) * 2).sum()
      val E_A = n * ((n * n - 3 * n + 3) * S_1) - n * S_2 + 3 * sum_weight * sum_weight
      val E_B = (arr_mean.map(t => t * t * t * t).sum / (rightdn * rightdn)) * ((n * n - n) * S_1 - 2 * n * S_2 + 6 * sum_weight * sum_weight)
      val E_C = (n - 1) * (n - 2) * (n - 3) * sum_weight * sum_weight
      val V_I = (E_A - E_B) / E_C - pow(E_I, 2)
      val Z_I = (moran_i - E_I) / sqrt(V_I)
      val gaussian = breeze.stats.distributions.Gaussian(0, 1)
      val Pvalue = 2 * (1.0 - gaussian.cdf(Z_I))
      //      println(s"global Moran's I is: $moran_i")
      //      println(s"Z-Score is: $Z_I , p-value is: $Pvalue")
      outStr += s"Z-Score is: ${Z_I.formatted("%.4f")} , "
      outStr += s"p-value is: ${Pvalue.formatted("%.6f")}"
    }
    println(outStr)
    outStr
  }

  /**
   * 输入RDD直接计算局部莫兰指数
   *
   * @param featureRDD  RDD
   * @param property 要计算的属性，String
   * @param adjust   是否调整n的取值。false(默认):n；true:n-1
   * @return RDD内含局部莫兰指数和预测值等
   */
  def localMoranI(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, adjust: Boolean = false):
  RDD[(String, (Geometry, Map[String, Any]))] = {
    val nb_weight = getNeighborWeight(featureRDD)
    val arr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue()).collect()
    val arr_mean = meandiff(arr)
    val arr_mul = arr_mean.map(t => {
      val re = arr_mean.clone()
      DenseVector(re)
    })
    val weight_m_arr = arrdvec2multi(nb_weight.collect(), arr_mul)
    val rightup = weight_m_arr.map(t => sum(t))
    val dvec_mean = DenseVector(arr_mean)
    var n = arr.length
    if (adjust) {
      n = arr.length - 1
    }
    val s2 = arr_mean.map(t => t * t).sum / n
    val lz = DenseVector(rightup)
    val z = dvec_mean
    val m2 = sum(dvec_mean * dvec_mean) / n

    val expectation = -z * z / ((n - 1) * m2)
    val local_moranI = z / s2 * lz

    val wi = DenseVector(nb_weight.map(t => sum(t)).collect())
    val wi2 = DenseVector(nb_weight.map(t => sum(t * t)).collect())
    //    val b2=((z*z*z*z).sum/ n)/ (s2*s2)
    //    val A= (n - b2) / (n - 1)
    //    val B= (2 * b2 - n) / ((n - 1) * (n - 2))
    //    val var_I = A * wi2 + B * (wi*wi - wi2) - expectation*expectation
    val var_I = (z / m2) * (z / m2) * (n / (n - 2.0)) * (wi2 - (wi * wi / (n - 1.0))) * (m2 - (z * z / (n - 1.0)))
    val Z_I = (local_moranI - expectation) / var_I.map(t => sqrt(t))
    val gaussian = breeze.stats.distributions.Gaussian(0, 1)
    val pv_I = Z_I.map(t => 2 * (1.0 - gaussian.cdf(t)))
    val featRDDidx = featureRDD.collect().zipWithIndex
//    println("************************")
    featRDDidx.map(t => {
      t._1._2._2 += ("local_moranI" -> local_moranI(t._2.toInt))
      t._1._2._2 += ("expectation" -> expectation(t._2.toInt))
      t._1._2._2 += ("local_var" -> var_I(t._2.toInt))
      t._1._2._2 += ("local_z" -> Z_I(t._2.toInt))
      t._1._2._2 += ("local_pv" -> pv_I(t._2.toInt))
    })
    sc.makeRDD(featRDDidx.map(_._1))
  }

  /**
   *
   * @param featureRDD  RDD
   * @param property    要计算的属性
   * @param test        是否检验，默认false
   * @param weightstyle 权重计算方式，默认为W（每个邻居权重都为1/邻居数）
   * @param knn         KNN邻接矩阵中邻居数量，默认为4
   * @return 全局Geary's C指数，检验指标等
   */
  def globalGearyC(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, test: Boolean = false, weightstyle: String = "W", knn: Int = 4) = {
    val rdd = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue())
    val arr = rdd.collect()
    var n = arr.length
    val rdd_meandiff = rddmeandiff(rdd)
    val arr_meandiff = rdd_meandiff.collect()

    val nb_weight = getKNearestNeighbourWeight(featureRDD, weightstyle, k = knn)
    val sum_weight = sumWeight(nb_weight) //S_0
    val rdd_multiplied = rdd.map(t => {
      val re = new Array[Double](n)
      for (i <- 0 until n) {
        re(i) = (t - arr(i)) * (t - arr(i))
      }
      DenseVector(re)
    })
    val weight_m_rdd = rrdvec2multi(nb_weight, rdd_multiplied.collect())
    val rightup = weight_m_rdd.map(t => sum(t)).sum
    val rightdn = rdd_meandiff.map(t => t * t).sum
    val geary_c = (n - 1) / (2 * sum_weight) * rightup / rightdn
    var outStr = f"global Geary's C is: ${geary_c.formatted("%.4f")}\n"

    if (test) {
      // non-randomisation test
      val S0 = sum_weight
      val arr_weight = nb_weight.zipWithIndex().collect()
      val sym_weight = arr_weight.map(t => {
        val t1 = t._1
        val i = t._2.toInt
        val re = new Array[Double](n)
        for (j <- 0 until n) {
          re(j) = t1(j) + arr_weight(j)._1(i)
        }
        DenseVector(re)
      })

      val S1 = 0.5 * sym_weight.map(t => t.map(u => u * u).sum).sum
      val S2 = arr_weight.map(t => {
        //val t1 = t._1
        val i = t._2.toInt
        val sum_col = (0 until n).map(j => arr_weight(i)._1(j)).sum
        val sum_row = (0 until n).map(j => arr_weight(j)._1(i)).sum
        pow(sum_col + sum_row, 2)
      }).sum
      val varC_up = (n - 1) * (2 * S1 + S2) - 4 * S0 * S0
      val varC_dn = 2 * (n + 1) * S0 * S0
      val varC = varC_up / varC_dn
      val E_C = 1

      val Z_I = (-geary_c + E_C) / sqrt(varC)
      val gaussian = breeze.stats.distributions.Gaussian(0, 1)
      val Pvalue = (1.0 - gaussian.cdf(Z_I))
      //      println(s"global Moran's I is: $moran_i")
      //      println(s"Z-Score is: $Z_I , p-value is: $Pvalue")
      outStr += s"Z-Score is: ${Z_I.formatted("%.4f")} , "
      outStr += s"p-value is: ${Pvalue.formatted("%.6f")} . "
      //      outStr += s"\nS0 is: ${S0.formatted("%.4f")} , "
      //      outStr += s"S1 is: ${S1.formatted("%.4f")} , "
      //      outStr += s"S2 is: ${S2.formatted("%.4f")} . "
      //      outStr += s"\nvarC is: ${varC.formatted("%.4f")} . "
    }

    println(outStr)
    outStr
  }

  /**
   *
   * @param featureRDD  RDD
   * @param property    要计算的属性
   * @param adjust      是否调整，是否调整n的取值。false(默认):n；true:n-1
   * @param weightstyle 权重计算方式，默认为W（每个邻居权重都为1/邻居数）,可输入值还包括"B""C""U"
   * @param knn         KNN邻接矩阵中邻居数量，默认为4
   * @param nsim        伪p值排列检验循环次数，默认为499
   * @return RDD内含局部Geary's C指数和预测值等
   */
  def localGearyC(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String,
                  adjust: Boolean = false, weightstyle: String = "W", knn: Int = 4, nsim: Int = 499) = {
    val nb_weight = getKNearestNeighbourWeight(featureRDD, weightstyle, k = knn)
    val arr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue()).collect()
    val arr_meandiff = meandiff(arr)

    var n = arr.length
    if (adjust) {
      n = arr.length - 1
    }
    val arr_diff2 = arr.map(t => {
      val re = arr.clone().map(u => (t - u) * (t - u))
      DenseVector(re)
    })
    val right = arrdvec2multi(nb_weight.collect(), arr_diff2).map(_.sum)
    val m2 = arr_meandiff.map(t => pow(t, 2)).sum / n
    val m4 = arr_meandiff.map(t => pow(t, 4)).sum / n
    val geary_c = right.map(_ / m2)

    //pseudo-p test
    val arr_indices = arr.indices.toArray
    val nb_indices = arr_indices.map(t => {
      arr_indices.take(t) ++ arr_indices.drop(t + 1)
    })
    //    val nsim = 499

    val weightMatrix = nb_weight.collect()
    val weightSums = weightMatrix.map(_.sum)

    val results = arr.indices.map(i => {
      val nb_idx = nb_indices(i)
      val val2shuffle = nb_idx.map(arr)

      val pTestRes = (0 until nsim).map { j => {
        // in evey test
        val shuffledVal = shuffle(val2shuffle)
        val arr_rdm = arr.clone
        nb_idx.zip(shuffledVal).foreach { case (idx, value) => arr_rdm(idx) = value }
        var sum = 0.0
        for (k <- nb_idx) {
          sum += weightMatrix(i)(k) * (arr_rdm(i) - arr_rdm(k)) * (arr_rdm(i) - arr_rdm(k))
        }
        sum / m2

      }
      }.toArray

      val E_C = pTestRes.sum / nsim
      val var_C = breeze.stats.variance(pTestRes)
      val C = geary_c(i)
      val Z_C = (C - E_C) / sqrt(var_C)

      val count_diffAbs = pTestRes.map(t => math.abs(t - E_C)).count(_ >= math.abs(C - E_C)).toDouble
      val count_less = pTestRes.count(_ <= C).toDouble
      val count_greater = pTestRes.count(_ >= C).toDouble
      //      val gaussian = breeze.stats.distributions.Gaussian(E_C, var_C)

      // Pr(z != E(Ci))
      val pValue_2sided = count_diffAbs / nsim

      // Pr(folded) Sim - 使用单侧检验
      val pValue_folded_sim = math.min((count_greater + 1) / (nsim + 1), (count_less + 1) / (nsim + 1))
      //        if (C > E_C) {
      //        (count_greater + 1)/(nsim + 1)
      //      } else {
      //        (count_less + 1)/(nsim + 1)
      //      }

      // Pr(z != E(Ci)) Sim
      val pValue_2sided_sim = 2 * pValue_folded_sim

      //skewness
      val skewness_up = pTestRes.map(t => pow(t - E_C, 3)).sum / nsim
      val skewness_dn = pow(var_C, 3.0 / 2.0)
      val skewness = skewness_up / skewness_dn

      //kurtosis
      val kurtosis_up = pTestRes.map(t => pow(t - E_C, 4)).sum / nsim
      val kurtosis_dn = var_C * var_C
      val kurtosis = kurtosis_up / kurtosis_dn - 3

      (i, E_C, var_C, Z_C, pValue_2sided, pValue_2sided_sim, pValue_folded_sim, skewness, kurtosis)
    }).toArray

    val featRDDidx = featureRDD.collect().zipWithIndex
    featRDDidx.map(t => {
      t._1._2._2 += ("geary_c" -> geary_c)
      t._1._2._2 += ("expectation" -> results.map(_._2))
      t._1._2._2 += ("var" -> results.map(_._3))
      t._1._2._2 += ("z" -> results.map(_._4))
      t._1._2._2 += ("pv" -> results.map(_._5))
      t._1._2._2 += ("pv_sim" -> results.map(_._6))
      t._1._2._2 += ("pv_fold_sim" -> results.map(_._7))
      t._1._2._2 += ("skewness" -> results.map(_._8))
      t._1._2._2 += ("kurtosis" -> results.map(_._9))
    })
    sc.makeRDD(featRDDidx.map(_._1))
  }

  /**
   *
   * @param sc          SparkContext
   * @param featureRDD  RDD
   * @param property    要计算的属性
   * @param weightstyle 权重计算方式，默认为W（每个邻居权重都为1/邻居数）,可输入值还包括"B""C""U"
   * @param knn         KNN邻接矩阵中邻居数量，默认为4
   * @param star        Boolean变量，默认为false，即计算Gi，若为true则计算Gi*
   * @return
   */
  def getisOrdG(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], property: String, weightstyle: String = "W",
                knn: Int = 4, star: Boolean = false) = {
    //    val knn = 4
    val nb_weight = getKNearestNeighbourWeight(featureRDD, weightstyle, k = knn, self_included = star)
    val arr = featureRDD.map(t => t._2._2(property).asInstanceOf[java.math.BigDecimal].doubleValue()).collect()
    val ave = arr.sum / arr.length
    val variance = arr.map(t => pow(t - ave, 2)).sum / arr.length
    val stddev = sqrt(variance)
    val n = arr.length
    //    val arr_meandiff = meandiff(arr)

    val Gi = if (star) //gi_star
      nb_weight.zipWithIndex().map { case (w, idx) => {
        val i = idx.toInt
        val sumW = w.sum
        val sumW2 = w.map(t => t * t).sum
        val sumWMultiX = (0 until n).map(j => {
          w(j) * arr(j)
        }).sum

        val gi_up = sumWMultiX - ave * sumW
        val gi_down = stddev * sqrt(((n) * sumW2 - sumW * sumW) / (n - 1))
        gi_up / gi_down
      }
      }
    else //gi
      nb_weight.zipWithIndex().map { case (w, idx) => {
        val i = idx.toInt
        val sumW = w.sum
        val sumW2 = w.map(t => t * t).sum
        val sumWMultiX = (0 until n).map(j => {
          w(j) * arr(j)
        }).sum

        val ave_i = (arr.sum - arr(i)) / (n - 1)
        val var_i = (arr.map(t => t * t).sum - pow(arr(i), 2)) / (n - 1) - pow(ave_i, 2)
        val stddev_i = sqrt(var_i)

        val gi_up = sumWMultiX - ave_i * sumW
        val gi_down = stddev_i * sqrt(((n - 1) * sumW2 - sumW * sumW) / (n - 2))
        gi_up / gi_down
      }
      }

    //    Gi.collect().foreach(println)

    val name_gi = if (star) "gi_star" else "gi"
    val featRDDidx = featureRDD.collect().zipWithIndex
    featRDDidx.map(t => {
      t._1._2._2 += (name_gi -> Gi)
    })
    sc.makeRDD(featRDDidx.map(_._1))

  }

  def plotmoran(x: Array[Double], w: RDD[DenseVector[Double]], morani: Double): Unit = {
    val xx = x
    val wx = w.map(t => t dot DenseVector(x)).collect()
    val f = Figure()
    val p = f.subplot(0)
    p += plot(xx, wx, '+')
    val xxmean = DenseVector.ones[Double](x.length) :*= (xx.sum / xx.length)
    val wxmean = DenseVector.ones[Double](x.length) :*= (wx.sum / wx.length)
    val xxy = linspace(wx.min - 2, wx.max + 2, x.length)
    val wxy = linspace(xx.min - 2, xx.max + 2, x.length)
    val x1 = DenseMatrix(DenseVector.ones[Double](x.length), DenseVector(x)).t
    val x1t = x1.t
    val x1ty = x1t * DenseVector(wx)
    val x1tx1 = x1t * x1
    val betas = inv(x1tx1) * x1ty
    val y = DenseMatrix(DenseVector.ones[Double](x.length), wxy).t * betas
    p.xlim = (xx.min - 2, xx.max + 2)
    p.ylim = (wx.min - 2, wx.max + 2)
    p += plot(wxy, y)
    p += plot(xxmean, xxy, lines = false, shapes = true, style = '.', colorcode = "[0,0,0]")
    p += plot(wxy, wxmean, lines = false, shapes = true, style = '.', colorcode = "[0,0,0]")
    p.xlabel = "x"
    p.ylabel = "wx"
    val printi = morani.formatted("%.4f")
    p.title = s"Global Moran's I is $printi"
  }

  def arrdvec2multi(arr1: Array[DenseVector[Double]], arr2: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
    val arr1idx = arr1.zipWithIndex
    arr1idx.map(t => {
      t._1 * arr2(t._2)
    })
  }

  def meandiff(arr: Array[Double]): Array[Double] = {
    val ave = arr.sum / arr.length
    arr.map(t => t - ave)
  }

  def rrdvec2multi(arr1: RDD[DenseVector[Double]], arr2: Array[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val arr1idx = arr1.zipWithIndex
    arr1idx.map(t => {
      t._1 * arr2(t._2.toInt)
    })
  }

  def rddmeandiff(input: RDD[Double]): RDD[Double] = {
    val ave = input.sum / input.count()
    input.map(t => t - ave)
  }

  def sumWeight(weightRDD: RDD[DenseVector[Double]]): Double = {
    weightRDD.map(t => sum(t)).sum()
  }

}
