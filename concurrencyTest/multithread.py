import threading


def execute_queries(KUSTO_QUER):
    try:
        # Execute the query
        RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)

        # Process the response
        for row in response.primary_results[0]:
            print(row)

    except KustoServiceError as error:
        print(f"Error executing query: {error}")
