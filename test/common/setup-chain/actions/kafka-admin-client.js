'use strict'

const {promisify} = require('util')
const Kafka = require('node-rdkafka')

let admin_number = 0

const DEFAULTS = {
  'metadata.broker.list': 'localhost:9092'
, 'security.protocol': 'plaintext'
, 'socket.timeout.ms': 10000
, 'request.timeout.ms': 10000
, 'socket.keepalive.enable': false
, 'retries': 1
, 'acks': 'all'
, 'compression.codec': 'gzip'
, 'topic_create_opts': {}
}

module.exports = async function kafkaAdminClient(opts, topic = '#topic') {
  opts = opts || {}

  topic = this.lookup(topic) // if topic is passed, a queue by that name will be created

  const {topic_create_opts, ...create_options} = this.lookup({
    ...DEFAULTS
  , ...opts
  })

  const kafka_admin_client = new Kafka.AdminClient.create({...create_options})
  this.registerKafkaAdminClient(`kafka-admin-${++admin_number}`, kafka_admin_client)

  // monkeypatch the promisified version of these since they are used in tests
  kafka_admin_client.createTopic = promisify(kafka_admin_client.createTopic)
  kafka_admin_client.deleteTopic = promisify(kafka_admin_client.deleteTopic)

  if (topic) {
    await kafka_admin_client.createTopic({
      topic
    , num_partitions: 1
    , replication_factor: 1
    , config: topic_create_opts
    })
    this.registerKafkaTopic(topic, kafka_admin_client)
  }

  return kafka_admin_client
}
