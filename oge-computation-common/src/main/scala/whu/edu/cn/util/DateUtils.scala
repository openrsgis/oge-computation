package whu.edu.cn.util

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try

object DateUtils {
  def parseDateTime(time: String): Option[LocalDateTime] = {
    val formatPatterns = List(
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-ddHH:mm:ss",
      "yyyy/MM/dd HH:mm:ss",
      "yyyy/MM/ddHH:mm:ss",
      "yyyy-MM-dd",
      "yyyy/MM/dd"
    )

    formatPatterns.view
      .map { pattern =>
        val formatter = DateTimeFormatter.ofPattern(pattern)
        // 优先尝试解析为 LocalDateTime
        Try(LocalDateTime.parse(time, formatter)).toOption
          .orElse(
            // 若失败，尝试解析为 LocalDate 并转换为当天的起始时间
            Try(LocalDate.parse(time, formatter)).map(_.atStartOfDay()).toOption
          )
      }
      .collectFirst { case Some(dt) => dt }
  }
}