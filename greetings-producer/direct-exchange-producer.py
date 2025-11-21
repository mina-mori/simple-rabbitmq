import pika
import uuid  # For generating unique correlation IDs

# Establish connection to RabbitMQ (localhost by default)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the request queue (idempotent; creates if it doesn't exist)
channel.queue_declare(queue='rpc_queue')

# Generate a unique correlation ID for this request
correlation_id = str(uuid.uuid4())

# Create a temporary exclusive queue for the response
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

# Response variable to store the result
response = None

# Callback function to handle the response
def on_response(ch, method, props, body):
    global response
    if props.correlation_id == correlation_id:  # Match the correlation ID
        response = body.decode()  # Decode the response message
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge receipt

# Subscribe to the callback queue and set the callback
channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=False)

# Publish the request message
request_message = "Get user data for ID 123"  # Example request
channel.basic_publish(
    exchange='',  # Default exchange
    routing_key='rpc_queue',  # Send to this queue
    properties=pika.BasicProperties(
        reply_to=callback_queue,  # Tell consumer where to send the response
        correlation_id=correlation_id,  # Unique ID for matching
    ),
    body=request_message.encode()  # Encode the message
)

print(f"Sent request: {request_message}")

# Wait for the response (blocks until received)
while response is None:
    connection.process_data_events()  # Non-blocking check for messages

print(f"Received response: {response}")

# Close the connection
connection.close()