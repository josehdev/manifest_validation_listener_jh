#!/usr/bin/env python3

import asyncio
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph import GraphServiceClient


def get_graph_service_client():
    """
    Creates and returns a graph service client authenticated with Azure.
    """    
    appCredential = ClientSecretCredential(
    )

    scopes = ['https://graph.microsoft.com/.default']

    graph_service_client = GraphServiceClient(appCredential, scopes)

    return graph_service_client


async def get_azure_user_by_email(graph_service_client: GraphServiceClient, email: str):
    """
    Finds and returns an Azure user object, by its email address.
    """    
    # Builds a query filter by mail property.
    # The equal (eq) comparison is non case-sensitive
    query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
            filter = f"mail eq '{email}'",
            count = True,
    )

    # Builds a request configuration object
    request_configuration = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters = query_params,
    )

    # Adds required header as per Microsoft documentation
    request_configuration.headers.add("ConsistencyLevel", "eventual")

    # Runs the users' query 
    result = await graph_service_client.users.get(request_configuration = request_configuration)

    if len(result.value) == 0:
        user = None
    else:
        user = result.value[0]

    return user


# Get graph service client
graph_service_client = get_graph_service_client()

email='jherrera@oakwoodsys.com'

# Get user id of guest user account
user = asyncio.run(get_azure_user_by_email(graph_service_client, email))
principal_id = user.id