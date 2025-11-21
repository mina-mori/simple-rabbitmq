import pika
import time

# Establish connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the fanout exchange
channel.exchange_declare(exchange='fanout_exchange', exchange_type='fanout')

# Declare a queue (let RabbitMQ name it, or specify one)
result = channel.queue_declare(queue='', exclusive=True)  # Exclusive for this consumer
queue_name = result.method.queue

# Bind the queue to the fanout exchange (routing key ignored)
channel.queue_bind(exchange='fanout_exchange', queue=queue_name)

print("Waiting for RPC requests via fanout exchange. To exit, press Ctrl+C")

# Callback function (same as before)
def on_request(ch, method, props, body):
    request = body.decode()
    print(f"Received request: {request}")
    
    # Simulate processing
    time.sleep(1)
    if "ID 123" in request:
        response = "Received. User data: {'name': 'John Doe', 'email': 'john@example.com'}"
    else:
        response = "Received. No data found for that ID."
    
    # Publish response back
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=response.encode()
    )
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"Sent response: {response}")

# Consume from the bound queue
channel.basic_consume(queue=queue_name, on_message_callback=on_request, auto_ack=False)

# Start consuming
channel.start_consuming()