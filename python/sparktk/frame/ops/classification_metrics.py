import pandas as pd
from sparktk.propobj import PropertiesObject

class ClassificationMetricsValue(PropertiesObject):
    """
    ClassificationMetricsValue class used to hold the data returned from classification_metrics().
    """
    def __init__(self, tc,  scala_result):
        self._tc = tc
        self._accuracy = scala_result.accuracy()
        cm = scala_result.confusionMatrix()
        if cm:
            self._confusion_matrix = cm
            column_list = self._tc.jutils.convert.from_scala_seq(cm.columnLabels())
            row_label_list = self._tc.jutils.convert.from_scala_seq(cm.rowLabels())
            header = ["Predicted_" + column.title() for column in column_list]
            row_index = ["Actual_" + row_label.title() for row_label in row_label_list]
            data = [list(x) for x in list(cm.getMatrix())]
            self._confusion_matrix = pd.DataFrame(data, index=row_index, columns=header)
        else:
            #empty pandas frame
            self._confusion_matrix = pd.DataFrame()

        self._f_measure = scala_result.fMeasure()
        self._precision = scala_result.precision()
        self._recall = scala_result.recall()

    @property
    def accuracy(self):
        return self._accuracy

    @property
    def confusion_matrix(self):
        return self._confusion_matrix

    @property
    def f_measure(self):
        return self._f_measure

    @property
    def precision(self):
        return self._precision

    @property
    def recall(self):
        return self._recall


def classification_metrics(self, label_column, pred_column, pos_label=None, beta=1.0, frequency_column=None):
    """
    Model statistics of accuracy, precision, and others.

    :param label_column: The name of the column containing the correct label for each instance.
    :param pred_column: The name of the column containing the predicted label for each instance.
    :param pos_label: This is a str or int for binary classifiers, and Null for multi-class classifiers.
                      The value to be interpreted as a positive instance.
    :param beta: This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is computed);
                 must be greater than zero. Defaults is 1.
    :param frequency_column: The name of an optional column containing the frequency of observations.
    :return: The data returned is composed of multiple components\:
             <object>.accuracy : double
             <object>.confusion_matrix : table
             <object>.f_measure : double
             <object>.precision : double
             <object>.recall : double

    Calculate the accuracy, precision, confusion_matrix, recall and :math:`F_{ \beta}` measure for a
    classification model.

    *   The **f_measure** result is the :math:`F_{ \beta}` measure for a
        classification model.
        The :math:`F_{ \beta}` measure of a binary classification model is the
        harmonic mean of precision and recall.
        If we let:

        * beta :math:`\equiv \beta`,
        * :math:`T_{P}` denotes the number of true positives,
        * :math:`F_{P}` denotes the number of false positives, and
        * :math:`F_{N}` denotes the number of false negatives

        then:

        .. math::

            F_{ \beta} = (1 + \beta ^ 2) * \frac{ \frac{T_{P}}{T_{P} + F_{P}} * \
            \frac{T_{P}}{T_{P} + F_{N}}}{ \beta ^ 2 * \frac{T_{P}}{T_{P} + \
            F_{P}}  + \frac{T_{P}}{T_{P} + F_{N}}}

        The :math:`F_{ \beta}` measure for a multi-class classification model is
        computed as the weighted average of the :math:`F_{ \beta}` measure for
        each label, where the weight is the number of instances of each label.
        The determination of binary vs. multi-class is automatically inferred
        from the data.

    *   The **recall** result of a binary classification model is the proportion
        of positive instances that are correctly identified.
        If we let :math:`T_{P}` denote the number of true positives and
        :math:`F_{N}` denote the number of false negatives, then the model
        recall is given by :math:`\frac {T_{P}} {T_{P} + F_{N}}`.

        For multi-class classification models, the recall measure is computed as
        the weighted average of the recall for each label, where the weight is
        the number of instances of each label.
        The determination of binary vs. multi-class is automatically inferred
        from the data.

    *   The **precision** of a binary classification model is the proportion of
        predicted positive instances that are correctly identified.
        If we let :math:`T_{P}` denote the number of true positives and
        :math:`F_{P}` denote the number of false positives, then the model
        precision is given by: :math:`\frac {T_{P}} {T_{P} + F_{P}}`.

        For multi-class classification models, the precision measure is computed
        as the weighted average of the precision for each label, where the
        weight is the number of instances of each label.
        The determination of binary vs. multi-class is automatically inferred
        from the data.

    *   The **accuracy** of a classification model is the proportion of
        predictions that are correctly identified.
        If we let :math:`T_{P}` denote the number of true positives,
        :math:`T_{N}` denote the number of true negatives, and :math:`K` denote
        the total number of classified instances, then the model accuracy is
        given by: :math:`\frac{T_{P} + T_{N}}{K}`.

        This measure applies to binary and multi-class classifiers.

    *   The **confusion_matrix** result is a confusion matrix for a
        binary classifier model, formatted for human readability.

    Notes
    -----
    The **confusion_matrix** is not yet implemented for multi-class classifiers.

    Examples
    --------
    Consider Frame *my_frame*, which contains the data

        <hide>
        >>> s = [('a', str),('b', int),('labels', int),('predictions', int)]
        >>> rows = [ ["red", 1, 0, 0], ["blue", 3, 1, 0],["green", 1, 0, 0],["green", 0, 1, 1]]
        >>> my_frame = tc.to_frame(rows, s)
        -etc-

        </hide>
        >>> my_frame.inspect()
        [#]  a      b  labels  predictions
        ==================================
        [0]  red    1       0            0
        [1]  blue   3       1            0
        [2]  green  1       0            0
        [3]  green  0       1            1


        >>> cm = my_frame.classification_metrics('labels', 'predictions', 1, 1)
        <progress>

        >>> cm.f_measure
        0.6666666666666666

        >>> cm.recall
        0.5

        >>> cm.accuracy
        0.75

        >>> cm.precision
        1.0

        >>> cm.confusion_matrix
                    Predicted_Pos  Predicted_Neg
        Actual_Pos              1              1
        Actual_Neg              0              2

    """
    return ClassificationMetricsValue(self._tc, self._scala.classificationMetrics(label_column,
                                      pred_column,
                                      self._tc.jutils.convert.to_scala_option_either_string_int(pos_label),
                                      float(beta),
                                      self._tc.jutils.convert.to_scala_option(frequency_column)))