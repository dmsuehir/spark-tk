/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ClassificationMetricsSummarization extends BaseFrame {

  def classificationMetrics(labelColumn: String,
                            predColumn: String,
                            posLabel: Option[Either[String, Int]] = None,
                            beta: Double = 1.0,
                            frequencyColumn: Option[String]): ClassificationMetricValue = {
    execute(ClassificationMetrics(labelColumn, predColumn, posLabel, beta, frequencyColumn))
  }
}

/**
 * Model statistics of accuracy, precision, and others.
 *
 * @param labelColumn The name of the column containing the correct label for each instance.
 * @param predColumn The name of the column containing the predicted label for each instance.
 * @param posLabel This is a str or int for binary classifiers, and Null for multi-class classifiers.
 *                 The value to be interpreted as a positive instance.
 * @param beta This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is
 *             computed); must be greater than zero. Default is 1.
 * @param frequencyColumn The name of an optional column containing the frequency of observations.
 */
case class ClassificationMetrics(labelColumn: String,
                                 predColumn: String,
                                 posLabel: Option[Either[String, Int]] = None,
                                 beta: Double,
                                 frequencyColumn: Option[String]) extends FrameSummarization[ClassificationMetricValue] {
  require(StringUtils.isNotEmpty(labelColumn), "label column is required")
  require(StringUtils.isNotEmpty(predColumn), "predict column is required")
  require(beta >= 0, "invalid beta value for f measure. Should be greater than or equal to 0")

  override def work(state: FrameState): ClassificationMetricValue = {
    // check if poslabel is an Int, string or None
    posLabel match {
      case Some(Left(stringPositiveLabel)) =>
        ClassificationMetricsFunctions.binaryClassificationMetrics(
          state,
          labelColumn,
          predColumn,
          stringPositiveLabel,
          beta,
          frequencyColumn
        )
      case Some(Right(intPositiveLabel)) => {
        ClassificationMetricsFunctions.binaryClassificationMetrics(
          state,
          labelColumn,
          predColumn,
          intPositiveLabel,
          beta,
          frequencyColumn
        )
      }
      case _ => {
        ClassificationMetricsFunctions.multiclassClassificationMetrics(
          state,
          labelColumn,
          predColumn,
          beta,
          frequencyColumn
        )
      }
    }
  }

}

/**
 * Classification metrics
 *
 * @param fMeasure Weighted average of precision and recall
 * @param accuracy Fraction of correct predictions
 * @param recall Fraction of positives correctly predicted
 * @param precision Fraction of correct predictions among positive predictions
 * @param confusionMatrix Matrix of actual vs. predicted classes
 */
case class ClassificationMetricValue(fMeasure: Double,
                                     accuracy: Double,
                                     recall: Double,
                                     precision: Double,
                                     confusionMatrix: ConfusionMatrix)