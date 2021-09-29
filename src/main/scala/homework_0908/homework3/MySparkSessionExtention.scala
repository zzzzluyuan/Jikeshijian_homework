package homework_0908.homework3

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtention extends (SparkSessionExtensions => Unit) {
  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectOptimizerRule(RepartitionInsert)
  }
}