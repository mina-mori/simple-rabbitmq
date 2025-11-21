import pika
import uuid

# Establish connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a fanout exchange (creates if it doesn't exist)
channel.exchange_declare(exchange='fanout_exchange', exchange_type='fanout')

# Create a temporary exclusive queue for the response
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

# Bind the callback queue to the fanout exchange (though fanout ignores routing keys, binding is required)
channel.queue_bind(exchange='fanout_exchange', queue=callback_queue)

# Response variable (now a list to handle multiple responses if needed)
responses = []

# Callback function to handle responses
def on_response(ch, method, props, body):
    response = body.decode()
    responses.append(response)
    print(f"Received response: {response}")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # In a real scenario, you might stop after the first response or collect all

# Subscribe to the callback queue
channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=False)

# Publish the request to the fanout exchange (routing key ignored)
request_message = "Get user data for ID 123"
correlation_id = str(uuid.uuid4())
channel.basic_publish(
    exchange='fanout_exchange',  # Publish to the fanout exchange
    routing_key='',  # Ignored by fanout, but required
    properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=correlation_id,
    ),
    body=request_message.encode()
)

print(f"Sent request to fanout exchange: {request_message}")

# Wait for responses (adjust logic as needed; here, wait for at least one)
while len(responses) < 1:  # Change to >1 if expecting multiple
    connection.process_data_events()

print(f"All responses: {responses}")

# Close the connection
connection.close()