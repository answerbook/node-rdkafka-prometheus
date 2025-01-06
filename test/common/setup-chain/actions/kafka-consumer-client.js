'use strict'

const {promisify} = require('util')
const {once} = require('events')
const Kafka = require('node-rdkafka')

let client_number = 0

const DEFAULTS = {
  'metadata.broker.list': 'localhost:9092'
, 'security.protocol': 'plaintext'
, 'socket.timeout.ms': 10000
, 'request.timeout.ms': 10000
, 'socket.keepalive.enable': true
, 'retries': 1
, 'acks': 'all'
, 'compression.codec': 'gzip'
, 'event_cb': true
, 'group.id': 'group'
, 'enable.auto.commit': false
, 'auto.offset.reset': 'earliest'
, 'fetch.wait.max.ms': 250
, 'fetch.queue.backoff.ms': 500
, 'topic.metadata.refresh.interval.ms': 1000
, 'topic.metadata.propagation.max.ms': 2000
, 'statistics.interval.ms': 1000
}

module.exports = async function kafkaConsumerClient(opts, topic = '#topic') {
  opts = opts || {}

  const create_options = this.lookup({
    ...DEFAULTS
  , ...opts
  })

  topic = this.lookup(topic) // if a topic is passed, it will be consumed immediately here

  const kafka_consumer_client = new Kafka.KafkaConsumer({...create_options}, {
    'auto.offset.reset': 'earliest'
  })

  const name = `kafka-consumer-${++client_number}`
  this.registerKafkaConsumerClient(name, kafka_consumer_client)

  const ready = once(kafka_consumer_client, 'ready')
  kafka_consumer_client.connect({timeout: 10000})
  await ready

  // Expose this promisified version for other tests
  kafka_consumer_client.getMetadata = promisify(kafka_consumer_client.getMetadata)

  if (topic) {
    await kafka_consumer_client.getMetadata({topic})

    const subscribed = once(kafka_consumer_client, 'subscribed')
    kafka_consumer_client.subscribe([topic])
    kafka_consumer_client.consume()
    await subscribed
  }

  return kafka_consumer_client
}
