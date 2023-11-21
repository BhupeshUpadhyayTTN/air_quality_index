import boto3
from Variables import database_info, aws_variables

def query_execute(*queries):

    # Create an Athena client
    athena_client = boto3.client('athena', aws_access_key_id=aws_variables['access_key'], aws_secret_access_key=aws_variables['secret_key'],region_name=aws_variables['athena']['region_name'])

    # query result location
    result_config={'OutputLocation': aws_variables['athena']['s3_query_output']}

    for query in queries:
        try:
            # Start query execution
            response = athena_client.start_query_execution(QueryString=query, ResultConfiguration=result_config)
            query_execution_id = response['QueryExecutionId']
            print(f"Query Execution ID: {query_execution_id}")

            # Wait for the query to complete
            while True:
                query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = query_execution['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break

            if status == 'SUCCEEDED':
                # Get query results
                result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
                result_data = result_response['ResultSet']['Rows']

                # Print the results
                print(f"Query result Output: {result_data}")

            else:
                print(f"Query execution failed with status: {status}")

        except Exception as e:
            print(f"Athena Error: {e}")


def athena_query():
    # Create a database in Athena
    create_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database_info['databaseName']}.{database_info['table']} (
        id INT,
        country STRING,
        state STRING,
        city STRING,
        station STRING,
        last_update DATE,
        average_pollutant_level double
        )
        PARTITIONED BY (pollutant_id STRING, day INT)
        STORED AS PARQUET
        LOCATION 's3://bhupeshbucket123/export/'
    """

    #refresh partitoin query
    partition_load_query = f"MSCK REPAIR TABLE {database_info['databaseName']}.{database_info['table']}"

    #Top 3 day wise most polluted city
    day_wise_most_polluted_city = f"""SELECT distinct(city) FROM (SELECT *, dense_rank() OVER(PARTITION BY 'day' ORDER BY 'average_pollutant_level') AS dense_ranking FROM {database_info['databaseName']}.{database_info['table']}) AS subquery WHERE dense_ranking < 6;"""
    
    #Top 3 day wise most polluted state
    day_wise_most_polluted_state = f"""SELECT distinct(state) FROM (SELECT *, dense_rank() OVER(PARTITION BY 'day' ORDER BY 'average_pollutant_level') AS dense_ranking FROM {database_info['databaseName']}.{database_info['table']}) AS subquery WHERE dense_ranking < 6;"""

    query_execute(create_table_query, partition_load_query, day_wise_most_polluted_city, day_wise_most_polluted_state)

if __name__ == '__main__':
    athena_query()
