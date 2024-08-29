import os

from confluent_kafka import Producer

# Configuration for the Kafka producer
conf = {
    'bootstrap.servers': 'localhost:29092',
}

# Create a producer instance
producer = Producer(conf)

# Directory containing Lua scripts
lua_scripts_dir = './lua_scripts/'


# Delivery callback for producer delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'\n pillai:kafka:producer: Message delivery failed: {err}')
    else:
        print(f'\n pillai:kafka:producer: Message delivered to {msg.topic()} [{msg.partition()}]')


# Produce messages for each Lua script in the directory
for script_name in os.listdir(lua_scripts_dir):
    if script_name.endswith('.lua'):
        with open(os.path.join(lua_scripts_dir, script_name), 'r') as script_file:
            script = script_file.read()
            # Produce a message
            producer.produce('lua_scripts', key=script_name, value=script, callback=delivery_report)

# Wait for any outstanding messages to be delivered
producer.flush()
