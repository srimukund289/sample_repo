CREATE OR REPLACE PROCEDURE SRIMUKUND.PUBLIC.GIT_TEST()
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import *
import snowflake.snowpark.functions as sf
# from snowflake.snowpark.functions import col

schema_for_file = StructType([
  StructField("id_sample", StringType()),
  StructField(''date_sample'', StringType())])

fileLocation = "@AWS_LOAD/sample1.csv"
outputTableName = "git_load_test"

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
      current_dtime = sf.current_timestamp()
      df_reader = session.read.schema(schema_for_file).option("skip_header", 1)
      df = df_reader.csv(fileLocation)
      df=df.withColumn("updated_at", current_dtime)
      df.write.mode("append").save_as_table(outputTableName)
      return df

      ';