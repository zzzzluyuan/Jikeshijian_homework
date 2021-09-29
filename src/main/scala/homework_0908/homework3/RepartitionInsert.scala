package homework_0908.homework3

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

case class RepartitionInsert(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logWarning("优化规则，RepartitionInsert")
    logWarning(plan.nodeName)
    logWarning(plan.children.map(_.nodeName).mkString(","))
    plan transformDown {
      case i@InsertIntoHiveTable(table, partition, plan@HiveTableRelation(_, _, _, _, _), overwrite, ifPartitionNotExists, outputColumnNames) =>
        i.withNewChildren(Seq(Repartition(1, shuffle = true, plan)))

    }
  }
}