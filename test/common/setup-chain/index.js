'use strict'

const dns = require('dns')
// fix for dns lookups on hosts with ipv4 and ipv6
// default to ipv4first
dns.setDefaultResultOrder('ipv4first')
const assert = require('assert')
const {promisify} = require('util')
const SetupChain = require('@logdna/setup-chain')
const actions = require('./actions/index.js')

class Chain extends SetupChain {
  constructor(opts) {
    super(opts, actions)

    this.kafka_admins = new Map()
    this.kafka_consumers = new Map()
    this.kafka_topics = new Map()
  }

  kafkaConsumerClient(opts, topic, label) {
    this.tasks.push(['kafkaConsumerClient', label, opts, topic])
    return this
  }

  kafkaAdminClient(opts, topic, label) {
    this.tasks.push(['kafkaAdminClient', label, opts, topic])
    return this
  }

  registerKafkaAdminClient(name, instance) {
    assert.ok(name, 'Kafka.Adminclient instance should have a "name" property!')
    this.kafka_admins.set(name, instance)
  }

  registerKafkaConsumerClient(name, instance) {
    assert.ok(name, 'Kafka.KafkaConsumer instance should have a "name" property!')
    this.kafka_consumers.set(name, instance)
  }

  registerKafkaTopic(name, admin) {
    this.kafka_topics.set(name, admin)
  }

  async teardown(t, {verbose} = {}) {
    // Special handling for promisifying
    for (const [name, consumer] of this.kafka_consumers.entries()) {
      if (t && verbose) t.comment(`Disconnecting Kafka consumer client: ${name}`)
      await promisify(consumer.disconnect.bind(consumer))()
    }
    for (const [topic, admin] of this.kafka_topics.entries()) {
      if (t && verbose) t.comment(`Deleting Kafka topic: ${topic}`)
      try {
        await admin.deleteTopic(topic)
      } catch (_) {
        // ignore errors
      }
    }
    for (const [name, admin] of this.kafka_admins.entries()) {
      if (t && verbose) t.comment(`Disconnecting Kafka admin client: ${name}`)
      admin.disconnect() // This is not a promise function
    }
  }

}

module.exports = function setupChain(state) {
  return new Chain(state)
}
