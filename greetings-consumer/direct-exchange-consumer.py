import pika
import time  # Simulate processing time

# Establish connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the request queue
channel.queue_declare(queue='rpc_queue')

print("Waiting for RPC requests. To exit, press Ctrl+C")

# Callback function to handle incoming requests
def on_request(ch, method, props, body):
    request = body.decode()  # Decode the request message
    print(f"Received request: {request}")
    
    # Simulate processing (e.g., fetching data)
    time.sleep(1)  # Fake delay
    if "ID 123" in request:
        response = "Received. User data: {'name': 'John Doe', 'email': 'john@example.com'}"  # Example response
    else:
        response = "Received. No data found for that ID."
    
    # Publish the response back to the reply queue
    ch.basic_publish(
        exchange='',  # Default exchange
        routing_key=props.reply_to,  # Send to the producer's reply queue
        properties=pika.BasicProperties(correlation_id=props.correlation_id),  # Match the correlation ID
        body=response.encode()  # Encode the response
    )
    
    # Acknowledge the request (removes it from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"Sent response: {response}")

# Subscribe to the queue and set the callback
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request, auto_ack=False)

# Start consuming (blocks indefinitely)
channel.start_consuming()