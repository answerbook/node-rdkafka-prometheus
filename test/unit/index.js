'use strict'

const tap = require('tap')
const RdkafkaStats = require('../../index.js')

tap.test('metrics', async (t) => {
  t.test('things should be the same name', async (t) => {
    const stat = new RdkafkaStats({})
    for (const metricKey of Object.keys(stat.metrics)) {
      t.same(stat.metrics[metricKey].name, `rdkafka_${metricKey.toLowerCase()}`)
    }
    t.ok(1)
  })
})
