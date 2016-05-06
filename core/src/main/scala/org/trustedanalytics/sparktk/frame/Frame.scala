package org.trustedanalytics.sparktk.frame

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.BaseFrame
import org.trustedanalytics.sparktk.frame.internal.ops._
import org.trustedanalytics.sparktk.frame.internal.ops.binning.{ BinColumnTransformWithResult, HistogramSummarization, QuantileBinColumnTransformWithResult }
import org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist.{ CumulativePercentTransform, CumulativeSumTransform, EcdfSummarization, TallyPercentTransform, TallyTransform }
import org.trustedanalytics.sparktk.frame.internal.ops.sample.AssignSampleTransform
import org.trustedanalytics.sparktk.frame.internal.ops.exportdata.ExportToCsvSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.flatten.FlattenColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.ops.RenameColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.ops.sortedk.SortedKSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation.{ CorrelationSummarization, CorrelationMatrixSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance.{ CovarianceMatrixSummarization, CovarianceSummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives.{ ColumnMedianSummarization, ColumnModeSummarization, ColumnSummaryStatisticsSummarization, CategoricalSummarySummarization }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.quantiles.QuantilesSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.topk.TopKSummarization
import org.trustedanalytics.sparktk.frame.internal.ops.unflatten.UnflattenColumnsTransform
import org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd

class Frame(frameRdd: RDD[Row], frameSchema: Schema) extends BaseFrame // params named "frameRdd" and "frameSchema" because naming them "rdd" and "schema" masks the base members "rdd" and "schema" in this scope
    with AddColumnsTransform
    with AssignSampleTransform
    with BinColumnTransformWithResult
    with CategoricalSummarySummarization
    with ColumnMedianSummarization
    with ColumnModeSummarization
    with ColumnSummaryStatisticsSummarization
    with CorrelationMatrixSummarization
    with CorrelationSummarization
    with CovarianceMatrixSummarization
    with CovarianceSummarization
    with CountSummarization
    with CumulativePercentTransform
    with CumulativeSumTransform
    with DropColumnsTransform
    with EcdfSummarization
    with ExportToCsvSummarization
    with FlattenColumnsTransform
    with HistogramSummarization
    with QuantilesSummarization
    with QuantileBinColumnTransformWithResult
    with RenameColumnsTransform
    with SaveSummarization
    with SortTransform
    with SortedKSummarization
    with TakeSummarization
    with TallyPercentTransform
    with TallyTransform
    with TopKSummarization
    with UnflattenColumnsTransform {
  init(frameRdd, frameSchema)

  /**
   * (typically called from pyspark, with jrdd)
   * @param jrdd java array of Any
   * @param schema frame schema
   */
  def this(jrdd: JavaRDD[Array[Any]], schema: Schema) = {
    this(PythonJavaRdd.toRowRdd(jrdd.rdd, schema), schema)
  }
}
