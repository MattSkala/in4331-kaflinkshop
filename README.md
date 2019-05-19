# KaflinkShop

This is an experimental project with the goal of implementing [reactive microservices](http://www.mammatustech.com/reactive-microservices) based on the [Kappa architecture](http://milinda.pathirage.org/kappa-architecture.com/) with [Apache Flink](https://flink.apache.org/), [Kafka](https://kafka.apache.org/) and [AIOHTTP](https://aiohttp.readthedocs.io/).

# HTTP Server

The HTTP server is implemented using AIOHTTP, an asynchronous HTTP client/server library for Python. 

## Installation
The application requires Python >= 3.7 and Apache Kafka running on the port `9092`.
1. Go to the api directory: `cd api`
1. Install dependencies with `pip install -r requirements.txt`
2. Start the server with `python app.py`
3. The server is running on the port `8080`

## Request Format

The server handles HTTP methods defined in the `app.router`. Each route handler first validates input parameters, then constructs the request object, and finally sends the request to a Kafka queue designated to the specific service.

All messages in Kafka should be serialized using JSON. Messages sent from the server to input topics should contain the following fields:

name | type | description
--- | --- | ---
`request_id` | `string` | A unique request ID that should also be included in the response
`sink` | `string` | The Kafka topic into which the response should be sent
`method` | `string` | The API method code
`params` | `object` | Request parameters

Example:
```
{
    "request_id": "4423f650-7a14-11e9-b9a4-80e6501cc886",
    "sink": "user_out_api1",
    "method": "find_user",
    "params": {
        "user_id": 1
    }
}
```

## Response Format

The response from Flink should be sent to the Kafka topic specified in the `sink` field. The response message should contain the `request_id` field matching the value in the request message, and any other fields that should be returned in the HTTP response.

Example:

```
{
    "request_id": "4423f650-7a14-11e9-b9a4-80e6501cc886",  
    "name": "Matt"
}
```

It the server does not receive the expected message in the sink within a specified timeout, the HTTP request fails.
