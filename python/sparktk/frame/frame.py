from pyspark.rdd import RDD

from sparktk.frame.pyframe import PythonFrame
from sparktk.frame.schema import schema_to_python, schema_to_scala
from sparktk import dtypes

# import constructors for the API's sake (not actually dependencies of the Frame class)
from sparktk.frame.constructors.create import create
from sparktk.frame.constructors.import_csv import import_csv
from sparktk.frame.constructors.import_jdbc import import_jdbc
from sparktk.frame.constructors.import_hive import import_hive

class Frame(object):

    def __init__(self, tc, source, schema=None, validate_schema=False):
        self._tc = tc
        if self._is_scala_frame(source):
            self._frame = source
        elif self.is_scala_rdd(source):
            scala_schema = schema_to_scala(tc.sc, schema)
            self._frame = self.create_scala_frame(tc.sc, source, scala_schema)
        else:
            if not isinstance(source, RDD):
                if isinstance(schema, list):
                    # check if schema is just a list of column names (versus string and data type tuples)
                    if all(isinstance(item, basestring) for item in schema):
                        schema = self._infer_schema(source, schema)
                elif schema is None:
                    schema = self._infer_schema(source)
                source = tc.sc.parallelize(source)
            if schema and validate_schema:
                # Validate schema by going through the data and checking the data type and attempting to parse it
                source = self.validate_pyrdd_schema(source, schema)
            self._frame = PythonFrame(source, schema)

    def _merge_types(self, type_list_a, type_list_b):
        """
        Merges two lists of data types

        :param type_list_a: First list of data types to merge
        :param type_list_b: Second list of data types to merge
        :return: List of merged data types
        """
        merged_types = []
        if not isinstance(type_list_a, list) or not isinstance(type_list_b, list):
            raise TypeError("Unable to generate schema, because schema is not a list.")
        if len(type_list_a) != len(type_list_b):
            raise ValueError("Length of each row must be the same (found rows with lengths: %s and %s)." % (len(type_list_a), len(type_list_b)))
        from sparktk import dtypes
        for i in range (0, len(type_list_a)):
            merged_type = dtypes._DataTypes.merge_types(type_list_a[i], type_list_b[i])
            merged_types.append(merged_type)
        return merged_types

    def _infer_types_for_row(self, row):
        """
        Returns a list of data types for the data in the specified row

        :param row: List or Row of data
        :return: List of data types
        """
        inferred_types = []
        for index, item in enumerate(row):
            if not isinstance(item, list):
                inferred_types.append(type(item))
            else:
                inferred_types.append(dtypes.vector((len(item))))
        return inferred_types

    def _infer_schema(self, data, column_names=[], sample_size=100):
        """
        Infers the schema based on the data in the RDD.

        :param sc: Spark Context
        :param data: Data used to infer schema
        :param column_names: Optional column names to use in the schema.  If no column names are provided, columns
                             are given numbered names.  If there are more columns in the RDD than there are in the
                             column_names list, remaining columns will be numbered.
        :param sample_size: Number of rows to check when inferring the schema.  Defaults to 100.
        :return: Schema
        """
        inferred_schema = []

        if isinstance(data, list):
            if len(data) > 0:
                # get the schema for the first row
                data_types = self._infer_types_for_row(data[0])

                if len(data) < sample_size:
                    sample_size = len(data)

                for i in range (1, sample_size):
                    data_types = self._merge_types(data_types, self._infer_types_for_row(data[i]))

                for i, data_type in enumerate(data_types):
                    column_name = "C%s" % i
                    if len(column_names) > i:
                        column_name = column_names[i]
                    inferred_schema.append((column_name, data_type))
        else:
            raise TypeError("Unable to infer schema, because the data provided is not a list.")
        return inferred_schema

    def validate_pyrdd_schema(self, pyrdd, schema):
        if isinstance(pyrdd, RDD):
            def validate_schema(row, schema):
                data = []
                if (len(row) != len(schema)):
                    raise ValueError("Length of the row (%s) does not match the schema length (%s)." % (len(row), len(schema)))

                for index, column in enumerate(schema):
                    data_type = column[1]
                    if isinstance(data_type, dtypes.vector):
                        vector_length = data_type.length
                        try:
                            data.append(dtypes.vector(vector_length).constructor(row[index]))
                        except:
                            # Unable to cast as vector
                            data.append(None)
                    elif not (isinstance(row[index], data_type)):
                        if data_type == int:
                            try:
                                data.append(int(row[index]))
                            except:
                                # Unable to cast value to an integer
                                data.append(None)
                        elif data_type == long:
                            try:
                                data.append(long(row[index]))
                            except:
                                # Unable to cast to a long
                                data.append(None)
                        elif data_type == float:
                            try:
                                data.append(float(row[index]))
                            except:
                                # Unable to cast to float
                                data.append(None)
                        elif data_type == str:
                            try:
                                data.append(str(row[index]))
                            except:
                                # Unable to cast to string
                                data.append(None)
                        elif data_type == unicode:
                            try:
                                data.append(unicode(row[index]))
                            except:
                                # Unable to cast to unicode
                                data.append(None)
                        else:
                            raise RuntimeError("Schema validation does not support data type: %s" % data_type)
                        # TODO: datetime
                    else:
                        # It's already the right data type, so use the original value.
                        data.append(row[index])
                return data
            return pyrdd.map(lambda row: validate_schema(row, schema))
        else:
            raise TypeError("Unable to validate schema, because the pyrdd provided is not an RDD.")

    @staticmethod
    def create_scala_frame(sc, scala_rdd, scala_schema):
        """call constructor in JVM"""
        return sc._jvm.org.trustedanalytics.sparktk.frame.Frame(scala_rdd, scala_schema, False)

    @staticmethod
    def load(tc, scala_frame):
        """creates a python Frame for the given scala Frame"""
        return Frame(tc, scala_frame)

    def _frame_to_scala(self, python_frame):
        """converts a PythonFrame to a Scala Frame"""
        scala_schema = schema_to_scala(self._tc.sc, python_frame.schema)
        scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.rdd.PythonJavaRdd.pythonToScala(python_frame.rdd._jrdd, scala_schema)
        return self.create_scala_frame(self._tc.sc, scala_rdd, scala_schema)

    def _is_scala_frame(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.Frame)

    def is_scala_rdd(self, item):
        return self._tc._jutils.is_jvm_instance_of(item, self._tc.sc._jvm.org.apache.spark.rdd.RDD)

    def is_python_rdd(self, item):
        return isinstance(item, RDD)

    @property
    def _is_scala(self):
        """answers whether the current frame is backed by a Scala Frame"""
        return self._is_scala_frame(self._frame)

    @property
    def _is_python(self):
        """answers whether the current frame is backed by a _PythonFrame"""
        return not self._is_scala

    @property
    def _scala(self):
        """gets frame backend as Scala Frame, causes conversion if it is current not"""
        if self._is_python:
            # convert PythonFrame to a Scala Frame"""
            scala_schema = schema_to_scala(self._tc.sc, self._frame.schema)
            scala_rdd = self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.pythonToScala(self._frame.rdd._jrdd, scala_schema)
            self._frame = self.create_scala_frame(self._tc.sc, scala_rdd, scala_schema)
        return self._frame

    @property
    def _python(self):
        """gets frame backend as _PythonFrame, causes conversion if it is current not"""
        if self._is_scala:
            # convert Scala Frame to a PythonFrame"""
            scala_schema = self._frame.schema()
            java_rdd =  self._tc.sc._jvm.org.trustedanalytics.sparktk.frame.internal.rdd.PythonJavaRdd.scalaToPython(self._frame.rdd())
            python_schema = schema_to_python(self._tc.sc, scala_schema)
            python_rdd = RDD(java_rdd, self._tc.sc)
            self._frame = PythonFrame(python_rdd, python_schema)
        return self._frame

    ##########################################################################
    # API
    ##########################################################################

    @property
    def rdd(self):
        """pyspark RDD  (causes conversion if currently backed by a Scala RDD)"""
        return self._python.rdd

    @property
    def schema(self):
        if self._is_scala:
            return schema_to_python(self._tc.sc, self._frame.schema())  # need ()'s on schema because it's a def in scala
        return self._frame.schema

    @property
    def column_names(self):
        """
        Column identifications in the current frame.

        :return: list of names of all the frame's columns

        Returns the names of the columns of the current frame.

        Examples
        --------

        .. code::

            >>> frame.column_names
            [u'name', u'age', u'tenure', u'phone']

        """
        return [name for name, data_type in self.schema]

    @property
    def row_count(self):
        """
        Number of rows in the current frame.

        :return: The number of rows in the frame

        Counts all of the rows in the frame.

        Examples
        --------
        Get the number of rows:

        <hide>
        frame = tc.frame.create([[item] for item in range(0, 4)],[("a", int)])
        </hide>

        .. code::

            >>> frame.row_count
            4

        """
        if self._is_scala:
            return int(self._scala.rowCount())
        return self.rdd.count()

    def append_csv_file(self, file_name, schema, separator=','):
        self._scala.appendCsvFile(file_name, schema_to_scala(self._tc.sc, schema), separator)

    def export_to_csv(self, file_name):
        self._scala.exportToCsv(file_name)

    # Frame Operations

    from sparktk.frame.ops.add_columns import add_columns
    from sparktk.frame.ops.append import append
    from sparktk.frame.ops.assign_sample import assign_sample
    from sparktk.frame.ops.bin_column import bin_column
    from sparktk.frame.ops.binary_classification_metrics import binary_classification_metrics
    from sparktk.frame.ops.categorical_summary import categorical_summary
    from sparktk.frame.ops.column_median import column_median
    from sparktk.frame.ops.column_mode import column_mode
    from sparktk.frame.ops.column_summary_statistics import column_summary_statistics
    from sparktk.frame.ops.copy import copy
    from sparktk.frame.ops.correlation import correlation
    from sparktk.frame.ops.correlation_matrix import correlation_matrix
    from sparktk.frame.ops.count import count
    from sparktk.frame.ops.covariance import covariance
    from sparktk.frame.ops.covariance_matrix import covariance_matrix
    from sparktk.frame.ops.cumulative_percent import cumulative_percent
    from sparktk.frame.ops.cumulative_sum import cumulative_sum
    from sparktk.frame.ops.dot_product import dot_product
    from sparktk.frame.ops.drop_columns import drop_columns
    from sparktk.frame.ops.drop_duplicates import drop_duplicates
    from sparktk.frame.ops.drop_rows import drop_rows
    from sparktk.frame.ops.ecdf import ecdf
    from sparktk.frame.ops.entropy import entropy
    from sparktk.frame.ops.export_data import export_to_jdbc, export_to_json, export_to_hbase, export_to_hive
    from sparktk.frame.ops.filter import filter
    from sparktk.frame.ops.flatten_columns import flatten_columns
    from sparktk.frame.ops.histogram import histogram
    from sparktk.frame.ops.inspect import inspect
    from sparktk.frame.ops.multiclass_classification_metrics import multiclass_classification_metrics
    from sparktk.frame.ops.quantile_bin_column import quantile_bin_column
    from sparktk.frame.ops.quantiles import quantiles
    from sparktk.frame.ops.rename_columns import rename_columns
    from sparktk.frame.ops.save import save
    from sparktk.frame.ops.sort import sort
    from sparktk.frame.ops.sortedk import sorted_k
    from sparktk.frame.ops.take import take
    from sparktk.frame.ops.tally import tally
    from sparktk.frame.ops.tally_percent import tally_percent
    from sparktk.frame.ops.topk import top_k
    from sparktk.frame.ops.unflatten_columns import unflatten_columns
