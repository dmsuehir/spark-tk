package org.trustedanalytics.sparktk.frame.internal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.Schema

import scala.util.{ Failure, Success }

trait BaseFrame {

  private var frameState: FrameState = null

  /**
   * The content of the frame as an RDD of Rows.
   */
  def rdd: RDD[Row] = if (frameState != null) frameState.rdd else null

  /**
   * Current frame column names and types.
   */
  def schema: Schema = if (frameState != null) frameState.schema else null

  /**
   * Validates the data against the specified schema. Attempts to parse the data to the column's data type.  If
   * it's unable to parse the data to the specified data type, an exception is thrown.
   *
   * @param rddToValidate RDD of data to validate against the specified schema
   * @param schemaToValidate Schema to use to validate the data
   * @return RDD that has data parsed to the schema's data types
   */
  protected def validateSchema(rddToValidate: RDD[Row], schemaToValidate: Schema): RDD[Row] = {
    val columnCount = schemaToValidate.columns.length
    val schemaWithIndex = schemaToValidate.columns.zipWithIndex

    rddToValidate.map(row => {
      if (row.length != columnCount)
        throw new RuntimeException(s"Row length of ${row.length} does not match the number of columns in the schema (${columnCount}).")

      val parsedValues = schemaWithIndex.map {
        case (column, index) =>
          column.dataType.parse(row.get(index)) match {
            case Success(value) => value
            case Failure(e) => null
          }
      }

      Row.fromSeq(parsedValues)
    })
  }

  protected def init(rdd: RDD[Row], schema: Schema): Unit = {
    frameState = FrameState(rdd, schema)
  }

  protected def execute(transform: FrameTransform): Unit = {
    frameState = transform.work(frameState)
  }

  protected def execute[T](summarization: FrameSummarization[T]): T = {
    summarization.work(frameState)
  }

  protected def execute[T](transform: FrameTransformWithResult[T]): T = {
    val r = transform.work(frameState)
    frameState = r.state
    r.result
  }
}

trait FrameOperation extends Product {
  //def name: String
}

trait FrameTransform extends FrameOperation {
  def work(state: FrameState): FrameState
}

case class FrameTransformReturn[T](state: FrameState, result: T)

trait FrameTransformWithResult[T] extends FrameOperation {
  def work(state: FrameState): FrameTransformReturn[T]
}

trait FrameSummarization[T] extends FrameOperation {
  def work(state: FrameState): T
}

trait FrameCreation extends FrameOperation {
  def work(): FrameState
}