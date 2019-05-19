from aiohttp import web
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import uuid
import asyncio


# distinguishes between Flink jobs and HTTP server services
SERVICE_ID = 'api1'

# topics for sending requests to Kafka
TOPIC_USERS_INPUT = 'user_in'

# topic for listening to responses from Kafka
TOPIC_USERS_OUTPUT = 'user_out_' + SERVICE_ID
OUTPUT_TOPICS = [TOPIC_USERS_OUTPUT]

# Kafka bootstrap server for both consumer and producer
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'


# request timeout in seconds
TIMEOUT = 60.0

# Stores Futures for all requests
requests = {}


async def start_kafka_producer(app):
    print('starting Kafka producer')
    producer = AIOKafkaProducer(
        loop=asyncio.get_running_loop(), 
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start()
    print('started Kafka producer')
    app['producer'] = producer


async def cleanup_kafka_producer(app):
    print('stopping Kafka producer')
    await app['producer'].stop()

async def listen_kafka_consumer(consumer):
    async for msg in consumer:        
        print('received response: ' + str(msg.value))

        try:
            response = json.loads(msg.value)        
            
            request_id = response['request_id']

            if (request_id in requests):
                print('response delivered to request #' + str(request_id))
                requests[request_id].set_result(response)
                del requests[request_id]
            else:
                print('no matching request found')
        except json.decoder.JSONDecodeError:
            print('parsing json failed')

async def start_kafka_consumer(app):
    print('starting Kafka consumer')
    consumer = AIOKafkaConsumer(*OUTPUT_TOPICS,
        loop=asyncio.get_running_loop(), 
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await consumer.start()
    print('started Kafka consumer')
    app['consumer'] = consumer
    asyncio.create_task(listen_kafka_consumer(consumer))

async def cleanup_kafka_consumer(app):
    print('stopping Kafka consumer')
    await app['consumer'].stop()


# Assigns a unique request ID and sends the request to the provided topic
# Returns a future that will return the response or times out
async def send_request(app, topic, request):
    # generate a UUID to identify a request
    request_id = str(uuid.uuid1())
    request['request_id'] = request_id

    # send the request to Kafka
    await app['producer'].send_and_wait(topic, bytes(json.dumps(request), 'utf-8'))

    # create a Future that will receive the response
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    requests[request_id] = fut

    # set request timeout
    try:
        print('sending request #' + request_id + ' ' + str(request))
        response = await asyncio.wait_for(fut, timeout=TIMEOUT)
        del response['request_id']
        return response
    except asyncio.TimeoutError:
        print('request #' + request_id + ' timed out')
        del requests[request_id]
        raise


async def hello(request):
    return web.Response(text='Welcome to KaflinkShop!')


def route_handler(route, input_topic, output_topic):
    async def handle(request):
        response = await send_request(request.app, input_topic, {
            'sink': output_topic,
            'route': route,
            'params': dict(request.match_info)
        })

        return web.json_response(response)

    return handle


app = web.Application()
app.router.add_get('/', hello)
app.router.add_post('/users/create',
    route_handler('users/create', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))
app.router.add_delete('/users/remove/{user_id}',
    route_handler('users/remove', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))
app.router.add_get('/users/find/{user_id}',
    route_handler('users/find', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))
app.router.add_get('/users/credit/{user_id}',
    route_handler('users/credit', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))
app.router.add_post('/users/credit/subtract/{user_id}/{amount}',
    route_handler('users/credit/subtract', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))
app.router.add_post('/users/credit/add/{user_id}/{amount}',
    route_handler('users/credit/add', TOPIC_USERS_INPUT, TOPIC_USERS_OUTPUT))

# https://aiohttp.readthedocs.io/en/stable/web_advanced.html#background-tasks
app.on_startup.append(start_kafka_producer)
app.on_startup.append(start_kafka_consumer)
app.on_cleanup.append(cleanup_kafka_producer)
app.on_cleanup.append(cleanup_kafka_consumer)

web.run_app(app)
