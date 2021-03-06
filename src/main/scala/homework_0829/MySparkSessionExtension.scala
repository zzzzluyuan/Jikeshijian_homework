package homework_0829

import homework.MyRule
import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      new MyRule(session)
    }
  }
}