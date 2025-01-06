'use strict'

const {once} = require('events')
const {promisify} = require('util')
const sleep = promisify(setTimeout)
const tap = require('tap')
const SetupChain = require('../common/setup-chain/index.js')
const RdkafkaStats = require('../../index.js')

tap.test('integration', async (t) => {
  t.test('get stats', async (t) => {

    const stat = new RdkafkaStats({})
    const chain = SetupChain()

    const state = await chain
      .kafkaAdminClient(
        null
      , 'test_topic'
      , 'admin_client'
      )
      .kafkaConsumerClient(
        null
      , 'test_topic'
      , 'consumer_client'
      )
      .execute()

    t.teardown(async () => {
      await chain.teardown()
    })

    state.consumer_client.on('event.stats', ((msg) => {
      const stats = JSON.parse(msg.message)
      stat.observe(stats)
    }))

    await once(state.consumer_client, 'event.stats')
    await sleep(500)

    const consumer_name = state.consumer_client.name
    const hash_key = `handle:${consumer_name},partition:0,topic:test_topic,type:consumer`
    // there should be zero lag (indicated by -1)
    t.same(-1, stat.metrics.TOPIC_PARTITION_CONSUMER_LAG.hashMap[hash_key].value)
  })
})
