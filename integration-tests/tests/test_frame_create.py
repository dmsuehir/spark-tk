from setup import tc, rm, get_sandbox_path
from sparktk import dtypes

def test_create_frame_with_column_names(tc):
    """
    Create a frame with a list of column names.  Data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]],["number", "letter", "decimal"])
    assert(frame.row_count == 3)
    assert(frame.schema == [("number", int), ("letter", str), ("decimal", float)])

def test_create_frame_without_schema(tc):
    """
    Creates a frame without a schema.  Column names should be numbered, and data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]])
    assert(frame.row_count == 3)
    assert(frame.schema == [("C0", int), ("C1", str), ("C2", float)])

def test_create_frame_with_not_enough_column_names(tc):
    """
    Creates a frame with a list of less column names than the amount of data.  The remaining column should
    be numbered.  Data types should be inferred.
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]],["number", "letter"])
    assert(frame.row_count == 3)
    assert(frame.schema == [("number", int), ("letter", str), ("C2", float)])

def test_create_frame_with_schema(tc):
    """
    Tests creating a frame with a custom schema
    """
    frame = tc.frame.create([[1,"a",1.5],[2,"b",5.0],[3,"c",22.7]], [("col_a", int),("col_b", str),("col_c", float)])
    assert(frame.row_count == 3)
    assert(frame.schema == [("col_a", int), ("col_b", str), ("col_c", float)])

def test_create_frame_with_vectors(tc):
    """
    Tests  creating a frame with vectors, as well as failing use case where the vectors aren't all the same length.
    """
    # Create a frame with vectors
    frame = tc.frame.create([[[1,2,3,0]],[[0,4,5,6]],[[7,8,9,10]]])
    assert(frame.row_count == 3)
    assert(len(frame.schema) == 1)
    assert(isinstance(frame.schema[0][1], dtypes.vector))   # schema data type should be a vector
    assert(frame.schema[0][1].length == 4)                  # vector length should be 4
    try:
        # Try to create a frame where vectors aren't all the same length
        tc.frame.create([[[1,2,3]],[[4,5,6]],[[7,8,9,10]]])
        raise RuntimeError("Expected exception when creating frame with different vector lengths.")
    except:
        pass

def test_create_with_schema_validation(tc):
    """
    Checks use cases with schema validation enabled.  Checks  use cases where we have all integers or a mix of integers
    and floats in the first 100 rows.  Verifies that in the case of mixed integers and floats, the integers should be
    casted to floats.  Checks for an exception in the use case where we have 100 integers followed by several strings
    that cannot be casted to integers.
    """
    # Test use case where we have more than 100 rows, all integers
    data = [[i] for i in xrange(0,110)]
    frame = tc.frame.create(data, validate_schema=True)
    assert(frame.row_count == 110)
    assert(frame.schema == [("C0", int)])
    assert(len(frame.take(frame.row_count).data) == frame.row_count)
    # Test use case where we have more than 100 rows, with a mix of integers and floats
    data = [[i] for i in xrange(0,55)] + [[i + .5] for i in xrange(0,55)]
    frame = tc.frame.create(data, validate_schema=True)
    assert(frame.row_count == 110)
    assert(frame.schema == [("C0", float)])
    data = frame.take(frame.row_count).data
    assert(len(data) == frame.row_count)
    assert(all(isinstance(row[0], float) for row in data))
    # Test use case where we have more than 100 rows of integers and then strings
    data = [[i] for i in xrange(0,100)] + [["xyz" + str(i)] for i in xrange(0,20)]
    try:
        frame = tc.frame.create(data, validate_schema=True)
        frame.take(frame.row_count).data
        raise RuntimeError("Expected exception due to strings not being able to be parsed as integers")
    except:
        pass

