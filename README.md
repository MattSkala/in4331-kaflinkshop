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

The server handles HTTP routes defined in the `app.router`. Each route handler constructs the request object and sends the request to a Kafka queue designated to the specific service. All messages in Kafka should be serialized using JSON and follow the signature outlined below.

```ts
interface Message {
	// input specifies message origin (from the web server)
	input: {
		request_id: string // e.g. "4423f650-7a14-11e9-b9a4-80e6501cc886"
		consumer: string   // input kafka topic
		route: string      // e.g. "users/find"
		params: {}         // any key-value pair is valid
	}
	// param represents params relevant for the current processing stage
	params?: {
		user_id?: string   // example parameter
        order_id?: string  // example parameter
        item_id?: string   // example parameter
		[key: string]: any // any key-value pair is valid
	}
	// state represent the current state of the message
	// not quite sure about parameters of state yet, tho
	state: {
		route: string
        sender: "web" | "user" | "order" | "stock" | "payment"
		state?: string | null
    }
    // the result is only present when sending a message back to the web server
	result?: {
		result: "success" | "failure" | "error"
		message?: string | null
		params: {}
	}
	// path describes what Flink processes the package has travelled through
	path: [{
		consumer: "web" | "user" | "order" | "stock" | "payment" // service to process the message
		route: string         // route
		state?: string | null // human-readable message
		params: {}            // params that denote state at this point
	}, {
		consumer: "web" | "user" | "order" | "stock" | "payment" 
		route: string
		state: string
		params: {}
	}]
}
```

There is an abstraction level provided in the current Java implementation, which allows one to deal with messages. See the existing code for examples.

## Response Format

The response from Flink should also be a message as defined above. The response message should contain the `input.request_id` field matching the value in the request message. The current level of abstraction takes care of this by default. 

If the server does not receive the expected message in the sink within a specified timeout, the HTTP request fails.
