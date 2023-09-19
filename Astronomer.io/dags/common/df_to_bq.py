"""Script to generate BigQuery schema based on Pandas DataFrame"""

from google.cloud.bigquery.schema import SchemaField

def generate_bq_schema(df, default_type="STRING"):
    """Given a passed dataframe, generate the associated Google BigQuery schema.
    copied from: https://github.com/pydata/pandas-gbq/blob/master/pandas_gbq/schema.py

    Arguments:
        dataframe : pandas.DataFrame
        default_type : string
            The default BigQuery type in case the type of the column
            does not exist in the schema.
    """

    # If you update this mapping, also update the table at
    # `docs/source/writing.rst`.
    types = {
        "i": "INTEGER",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }

    client_schema = [SchemaField(col, types.get(dtype.kind, default_type)) for col, dtype in df.dtypes.iteritems()]

    pandas_schema = [{'name': col, 'type': types.get(dtype.kind, default_type)} for col, dtype in df.dtypes.iteritems()]
    
    return client_schema, pandas_schema


if __name__ == '__main__':
    pass