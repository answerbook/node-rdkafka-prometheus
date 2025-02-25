'use strict'
/* eslint-disable max-len */

const prometheus = require('prom-client')
const logger = require('@log4js-node/log4js-api').getLogger('node-rdkafka-prometheus')

/**
 * @typedef {Object} Options
 * @property {Registry[]} [registers] prometheus registries
 * @property {Object.<string,string>} [extraLabels={}] additional labels to apply to the metrics
 * @property {string} [namePrefix=''] prefix for metric names
 */

/**
 * Topic fetch states
 *
 * The order matches the order in rdkafka's `rd_kafka_fetch_states`.
 *
 * See https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_partition.c rd_kafka_fetch_states
 */
const FETCH_STATES = ['none', 'stopping', 'stopped', 'offset-query', 'offset-wait', 'active']

/**
 * Broker states
 *
 * The order matches the order in rdkafka's `rd_kafka_broker_state_names`.
 *
 * @see https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_broker.c rd_kafka_broker_state_names
 */
const BROKER_STATES = ['INIT', 'DOWN', 'CONNECT', 'AUTH', 'UP', 'UPDATE', 'APIVERSION_QUERY', 'AUTH_HANDSHAKE']

/**
 * A "metric" that observes rdkafka statistics
 */
class RdkafkaStats {
  /**
  * Create the collector
  *
  * @param {Options} options options for the collector
  */
  constructor(options) {
    const {extraLabels, namePrefix, registers} = {extraLabels: {}
    , namePrefix: ''
    , registers: [prometheus.register], ...options}

    this.registers = registers

    const globalLabelNames = ['handle', 'type', ...Object.keys(extraLabels)]
    const brokerLabelNames = [...globalLabelNames, 'name', 'nodeid']
    const topparsLabelNames = [...brokerLabelNames, 'topic']
    const topicLabelNames = [...globalLabelNames, 'topic']
    const topicPartitionLabelNames = [...topicLabelNames, 'partition']
    const cgrpLabelNames = [...globalLabelNames]

    // Disable eslint from complaining about the order: this is based on what rdkafka has in the documentation, so make finding specific statistics faster.
    /* eslint-disable sort-keys */
    this.metrics = {
      // Top-level metrics
      TS: this.makeRdKafkaCounter({
        help: 'librdkafka\'s internal monotonic clock (micro seconds)'
      , name: `${namePrefix}rdkafka_ts`
      , labelNames: globalLabelNames
      })
    , TIME: this.makeRdKafkaCounter({
        help: 'Wall clock time in seconds since the epoch'
      , name: `${namePrefix}rdkafka_time`
      , labelNames: globalLabelNames
      })
    , REPLYQ: this.makeRdkafkaGauge({
        help: 'Number of ops waiting in queue for application to serve with rd_kafka_poll()'
      , name: `${namePrefix}rdkafka_replyq`
      , labelNames: globalLabelNames
      })
    , MSG_CNT: this.makeRdkafkaGauge({
        help: 'Current number of messages in instance queues'
      , name: `${namePrefix}rdkafka_msg_cnt`
      , labelNames: globalLabelNames
      })
    , MSG_SIZE: this.makeRdkafkaGauge({
        help: 'Current total size of messages in instance queues'
      , name: `${namePrefix}rdkafka_msg_size`
      , labelNames: globalLabelNames
      })
    , MSG_MAX: this.makeRdkafkaGauge({
        help: 'Threshold: maximum number of messages allowed'
      , name: `${namePrefix}rdkafka_msg_max`
      , labelNames: globalLabelNames
      })
    , MSG_SIZE_MAX: this.makeRdkafkaGauge({
        help: 'Threshold: maximum total size of messages allowed'
      , name: `${namePrefix}rdkafka_msg_size_max`
      , labelNames: globalLabelNames
      })
    , SIMPLE_CNT: this.makeRdkafkaGauge({
        help: 'Internal tracking of legacy vs new consumer API state'
      , name: `${namePrefix}rdkafka_simple_cnt`
      , labelNames: globalLabelNames
      })
    , METADATA_CACHE_CNT: this.makeRdKafkaCounter({
        help: 'Number of topics in the metadata cache'
      , name: `${namePrefix}rdkafka_metadata_cache_cnt`
      , labelNames: globalLabelNames
      })

      // Per-Broker metrics
    , BROKER_STATE: this.makeRdkafkaGauge({
        help: `Broker state (${BROKER_STATES.map((value, index) => { return `${index} = ${value}` }).join(',')})`
      , name: `${namePrefix}rdkafka_broker_state`
      , labelNames: brokerLabelNames
      })
    , BROKER_STATEAGE: this.makeRdkafkaGauge({
        help: 'Time since last broker state change (microseconds)'
      , name: `${namePrefix}rdkafka_broker_stateage`
      , labelNames: brokerLabelNames
      })
    , BROKER_OUTBUF_CNT: this.makeRdkafkaGauge({
        help: 'Number of requests awaiting transmission to broker'
      , name: `${namePrefix}rdkafka_broker_outbuf_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_OUTBUF_MSG_CNT: this.makeRdkafkaGauge({
        help: 'Number of messages in outbuf_cnt'
      , name: `${namePrefix}rdkafka_broker_outbuf_msg_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_WAITRESP_CNT: this.makeRdkafkaGauge({
        help: 'Number of requests in-flight to broker awaiting response'
      , name: `${namePrefix}rdkafka_broker_waitresp_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_WAITRESP_MSG_CNT: this.makeRdkafkaGauge({
        help: 'Number of messages in waitresp_cnt'
      , name: `${namePrefix}rdkafka_broker_waitresp_msg_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_TX: this.makeRdKafkaCounter({
        help: 'Total number of requests sent'
      , name: `${namePrefix}rdkafka_broker_tx`
      , labelNames: brokerLabelNames
      })
    , BROKER_TXBYTES: this.makeRdKafkaCounter({
        help: 'Total number of bytes sent'
      , name: `${namePrefix}rdkafka_broker_txbytes`
      , labelNames: brokerLabelNames
      })
    , BROKER_TXERRS: this.makeRdKafkaCounter({
        help: 'Total number of transmissions errors'
      , name: `${namePrefix}rdkafka_broker_txerrs`
      , labelNames: brokerLabelNames
      })
    , BROKER_TXRETRIES: this.makeRdKafkaCounter({
        help: 'Total number of request retries'
      , name: `${namePrefix}rdkafka_broker_txretries`
      , labelNames: brokerLabelNames
      })
    , BROKER_REQ_TIMEOUTS: this.makeRdKafkaCounter({
        help: 'Total number of requests timed out'
      , name: `${namePrefix}rdkafka_broker_req_timeouts`
      , labelNames: brokerLabelNames
      })
    , BROKER_RX: this.makeRdKafkaCounter({
        help: 'Total number of responses received'
      , name: `${namePrefix}rdkafka_broker_rx`
      , labelNames: brokerLabelNames
      })
    , BROKER_RXBYTES: this.makeRdKafkaCounter({
        help: 'Total number of bytes received'
      , name: `${namePrefix}rdkafka_broker_rxbytes`
      , labelNames: brokerLabelNames
      })
    , BROKER_RXERRS: this.makeRdKafkaCounter({
        help: 'Total number of receive errors'
      , name: `${namePrefix}rdkafka_broker_rxerrs`
      , labelNames: brokerLabelNames
      })
    , BROKER_RXCORRIDERRS: this.makeRdKafkaCounter({
        help: 'Total number of unmatched correlation ids in response (typically for timed out requests)'
      , name: `${namePrefix}rdkafka_broker_rxcorriderrs`
      , labelNames: brokerLabelNames
      })
    , BROKER_RXPARTIAL: this.makeRdKafkaCounter({
        help: 'Total number of partial messagesets received'
      , name: `${namePrefix}rdkafka_broker_rxpartial`
      , labelNames: brokerLabelNames
      })
    , BROKER_ZBUF_GROW: this.makeRdKafkaCounter({
        help: 'Total number of decompression buffer size increases'
      , name: `${namePrefix}rdkafka_broker_zbuf_grow`
      , labelNames: brokerLabelNames
      })
    , BROKER_BUF_GROW: this.makeRdKafkaCounter({
        help: 'Total number of buffer size increases'
      , name: `${namePrefix}rdkafka_broker_buf_grow`
      , labelNames: brokerLabelNames
      })
    , BROKER_WAKEUPS: this.makeRdKafkaCounter({
        help: 'Broker thread poll wakeups'
      , name: `${namePrefix}rdkafka_broker_wakeups`
      , labelNames: brokerLabelNames
      })
      // Window stats: each of (int_latency,rtt,throttle) x (min, max, avg, sum, cnt)
    , BROKER_INT_LATENCY_MIN: this.makeRdkafkaGauge({
        help: 'Internal producer queue latency in microseconds (smallest value)'
      , name: `${namePrefix}rdkafka_broker_int_latency_min`
      , labelNames: brokerLabelNames
      })
    , BROKER_INT_LATENCY_MAX: this.makeRdkafkaGauge({
        help: 'Internal producer queue latency in microseconds (largest value)'
      , name: `${namePrefix}rdkafka_broker_int_latency_max`
      , labelNames: brokerLabelNames
      })
    , BROKER_INT_LATENCY_AVG: this.makeRdkafkaGauge({
        help: 'Internal producer queue latency in microseconds (average value)'
      , name: `${namePrefix}rdkafka_broker_int_latency_avg`
      , labelNames: brokerLabelNames
      })
    , BROKER_INT_LATENCY_SUM: this.makeRdkafkaGauge({
        help: 'Internal producer queue latency in microseconds (sum of values)'
      , name: `${namePrefix}rdkafka_broker_int_latency_sum`
      , labelNames: brokerLabelNames
      })
    , BROKER_INT_LATENCY_CNT: this.makeRdkafkaGauge({
        help: 'Internal producer queue latency in microseconds (number of value samples)'
      , name: `${namePrefix}rdkafka_broker_int_latency_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_RTT_MIN: this.makeRdkafkaGauge({
        help: 'Broker latency / round-trip time in microseconds (smallest value)'
      , name: `${namePrefix}rdkafka_broker_rtt_min`
      , labelNames: brokerLabelNames
      })
    , BROKER_RTT_MAX: this.makeRdkafkaGauge({
        help: 'Broker latency / round-trip time in microseconds (largest value)'
      , name: `${namePrefix}rdkafka_broker_rtt_max`
      , labelNames: brokerLabelNames
      })
    , BROKER_RTT_AVG: this.makeRdkafkaGauge({
        help: 'Broker latency / round-trip time in microseconds (average value)'
      , name: `${namePrefix}rdkafka_broker_rtt_avg`
      , labelNames: brokerLabelNames
      })
    , BROKER_RTT_SUM: this.makeRdkafkaGauge({
        help: 'Broker latency / round-trip time in microseconds (sum of values)'
      , name: `${namePrefix}rdkafka_broker_rtt_sum`
      , labelNames: brokerLabelNames
      })
    , BROKER_RTT_CNT: this.makeRdkafkaGauge({
        help: 'Broker latency / round-trip time in microseconds (number of value samples)'
      , name: `${namePrefix}rdkafka_broker_rtt_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_THROTTLE_MIN: this.makeRdkafkaGauge({
        help: 'Broker throttling time in milliseconds (smallest value)'
      , name: `${namePrefix}rdkafka_broker_throttle_min`
      , labelNames: brokerLabelNames
      })
    , BROKER_THROTTLE_MAX: this.makeRdkafkaGauge({
        help: 'Broker throttling time in milliseconds (largest value)'
      , name: `${namePrefix}rdkafka_broker_throttle_max`
      , labelNames: brokerLabelNames
      })
    , BROKER_THROTTLE_AVG: this.makeRdkafkaGauge({
        help: 'Broker throttling time in milliseconds (average value)'
      , name: `${namePrefix}rdkafka_broker_throttle_avg`
      , labelNames: brokerLabelNames
      })
    , BROKER_THROTTLE_SUM: this.makeRdkafkaGauge({
        help: 'Broker throttling time in milliseconds (sum of values)'
      , name: `${namePrefix}rdkafka_broker_throttle_sum`
      , labelNames: brokerLabelNames
      })
    , BROKER_THROTTLE_CNT: this.makeRdkafkaGauge({
        help: 'Broker throttling time in milliseconds (number of value samples)'
      , name: `${namePrefix}rdkafka_broker_throttle_cnt`
      , labelNames: brokerLabelNames
      })
    , BROKER_TOPPARS_PARTITION: this.makeRdKafkaCounter({
        help: 'Partitions handled by this broker handle'
      , name: `${namePrefix}rdkafka_broker_toppars_partition`
      , labelNames: topparsLabelNames
      })
      // Per-Topic metrics
    , TOPIC_METADATA_AGE: this.makeRdkafkaGauge({
        help: 'Age of metadata from broker for this topic (milliseconds)'
      , name: `${namePrefix}rdkafka_topic_metadata_age`
      , labelNames: topicLabelNames
      })
      // Per-Topic-Per-Partition metrics
    , TOPIC_PARTITION_LEADER: this.makeRdkafkaGauge({
        help: 'Current leader broker id'
      , name: `${namePrefix}rdkafka_topic_partition_leader`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_DESIRED: this.makeRdkafkaGauge({
        help: 'Partition is explicitly desired by application (1 = true, 0 = false)'
      , name: `${namePrefix}rdkafka_topic_partition_desired`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_UNKNOWN: this.makeRdkafkaGauge({
        help: 'Partition is not seen in topic metadata from broker (1 = true, 0 = false)'
      , name: `${namePrefix}rdkafka_topic_partition_unknown`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_MSGQ_CNT: this.makeRdkafkaGauge({
        help: 'Number of messages waiting to be produced in first-level queue'
      , name: `${namePrefix}rdkafka_topic_partition_msgq_cnt`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_MSGQ_BYTES: this.makeRdkafkaGauge({
        help: 'Number of bytes in msgq_cnt'
      , name: `${namePrefix}rdkafka_topic_partition_msgq_bytes`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_XMIT_MSGQ_CNT: this.makeRdkafkaGauge({
        help: 'Number of messages ready to be produced in transmit queue'
      , name: `${namePrefix}rdkafka_topic_partition_xmit_msgq_cnt`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_XMIT_MSGQ_BYTES: this.makeRdkafkaGauge({
        help: 'Number of bytes in xmit_msqg'
      , name: `${namePrefix}rdkafka_topic_partition_xmit_msgq_bytes`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_FETCHQ_CNT: this.makeRdkafkaGauge({
        help: 'Number of pre-fetched messages in fetch queue'
      , name: `${namePrefix}rdkafka_topic_partition_fetchq_cnt`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_FETCHQ_SIZE: this.makeRdkafkaGauge({
        help: 'Bytes in fetchq'
      , name: `${namePrefix}rdkafka_topic_partition_fetchq_size`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_FETCH_STATE: this.makeRdkafkaGauge({
        help: `Consumer fetch state for this partition (${FETCH_STATES.map((value, index) => { return `${index} = ${value}` }).join(',')})`
      , name: `${namePrefix}rdkafka_topic_partition_fetch_state`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_QUERY_OFFSET: this.makeRdkafkaGauge({
        help: 'Current/Last logical offset query'
      , name: `${namePrefix}rdkafka_topic_partition_query_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_NEXT_OFFSET: this.makeRdkafkaGauge({
        help: 'Next offset to fetch'
      , name: `${namePrefix}rdkafka_topic_partition_next_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_APP_OFFSET: this.makeRdkafkaGauge({
        help: 'Offset of last message passed to application'
      , name: `${namePrefix}rdkafka_topic_partition_app_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_STORED_OFFSET: this.makeRdkafkaGauge({
        help: 'Offset to be committed'
      , name: `${namePrefix}rdkafka_topic_partition_stored_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_COMMITTED_OFFSET: this.makeRdkafkaGauge({
        help: 'Last committed offset'
      , name: `${namePrefix}rdkafka_topic_partition_committed_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_EOF_OFFSET: this.makeRdkafkaGauge({
        help: 'Last PARTITION_EOF signaled offset'
      , name: `${namePrefix}rdkafka_topic_partition_eof_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_LO_OFFSET: this.makeRdkafkaGauge({
        help: 'Partition\'s low watermark offset on broker'
      , name: `${namePrefix}rdkafka_topic_partition_lo_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_HI_OFFSET: this.makeRdkafkaGauge({
        help: 'Partition\'s high watermark offset on broker'
      , name: `${namePrefix}rdkafka_topic_partition_hi_offset`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_CONSUMER_LAG: this.makeRdkafkaGauge({
        help: 'Difference between hi_offset - app_offset'
      , name: `${namePrefix}rdkafka_topic_partition_consumer_lag`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_TXMSGS: this.makeRdKafkaCounter({
        help: 'Total number of messages transmitted (produced)'
      , name: `${namePrefix}rdkafka_topic_partition_txmsgs`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_TXBYTES: this.makeRdKafkaCounter({
        help: 'Total number of bytes transmitted'
      , name: `${namePrefix}rdkafka_topic_partition_txbytes`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_MSGS: this.makeRdKafkaCounter({
        help: 'Total number of messages received (consumed)'
      , name: `${namePrefix}rdkafka_topic_partition_msgs`
      , labelNames: topicPartitionLabelNames
      })
    , TOPIC_PARTITION_RX_VER_DROPS: this.makeRdKafkaCounter({
        help: 'Dropped outdated messages'
      , name: `${namePrefix}rdkafka_topic_partition_rx_ver_drops`
      , labelNames: topicPartitionLabelNames
      })
      // Consumer group metrics
    , CGRP_REBALANCE_AGE: this.makeRdkafkaGauge({
        help: 'Time elapsed since last rebalance (assign or revoke) (milliseconds)'
      , name: `${namePrefix}rdkafka_cgrp_rebalance_age`
      , labelNames: cgrpLabelNames
      })
    , CGRP_REBALANCE_CNT: this.makeRdkafkaGauge({
        help: 'Total number of rebalances (assign or revoke)'
      , name: `${namePrefix}rdkafka_cgrp_rebalance_cnt`
      , labelNames: cgrpLabelNames
      })
    , CGRP_ASSIGNMENT_SIZE: this.makeRdkafkaGauge({
        help: 'Current assignment\'s partition count'
      , name: `${namePrefix}rdkafka_cgrp_assignment_size`
      , labelNames: cgrpLabelNames
      })
    }
    /* eslint-enable sort-keys */
    this.extraLabels = extraLabels

    /**
   * Set of names of metrics that were unknown and we have warned the user about.
   */
    this.warnedUnknownMetrics = new Set()
  }
  // Note that rdkafka classifies metrics as type 'counter' (or 'int'), but effectively we cannot use
  // prometheus.Counter here, as that only allows incrementing (rightfully). We need a prometheus.Gauge, as we're just
  // reporting whatever rdkafka tells.
  // At the same time all rdkafka 'gauge' metrics could be histograms for us, where we'd record the seen values over time. This would lead to
  // issues in having to define the buckets though, and would make it harder to produce "current" statistics.
  // Abstract this into two functions so it can be looked at later.
  makeRdKafkaCounter(counterOptions) {
    return new prometheus.Gauge(Object.assign(counterOptions, {registers: this.registers}))
  }
  makeRdkafkaGauge(gaugeOptions) {
    return new prometheus.Gauge(Object.assign(gaugeOptions, {registers: this.registers}))
  }
  _translateRdkafkaStat(key, value, labels, valueMapper = (v) => { return v }) {
    const metric = this.metrics[key.toUpperCase()]
    if (metric) {
      try {
		/* istanbul ignore next */
        const observe = 'observe' in metric ? 'observe' : 'set'
        metric[observe](labels, valueMapper(value))
      } catch (e) { /* istanbul ignore next */
        // FIXME: prometheus.Counter doesn't have a "set value to", but only a "inc value by".
        logger.warn(`Cannot determine how to observice metric ${metric.name}`)
      }
    } else if (!this.warnedUnknownMetrics.has(key)) {
      this.warnedUnknownMetrics.add(key)
      logger.warn(`Unknown metric ${key} (labels ${JSON.stringify(labels)})`)
    }
  }

  _translateRdKafkaBrokerWindowStats(windowKey, brokerWindowStats, brokerLabels) {
    for (const key of Object.keys(brokerWindowStats)) {
      this._translateRdkafkaStat(`broker_${windowKey}_${key}`, brokerWindowStats[key], brokerLabels)
    }
  }

  _translateRdKafkaBrokerTopparsStats(brokerTopparsStats, brokerLabels) {
    const brokerTopparsLabels = {...brokerLabels, topic: brokerTopparsStats.topic}

    for (const key of Object.keys(brokerTopparsStats)) {
      switch (key) {
        case 'topic':
          // Ignore: part of brokerTopparsLabels
          break
        default:
          this._translateRdkafkaStat(`broker_toppars_${key}`, brokerTopparsStats[key], brokerTopparsLabels)
          break
      }
    }
  }

  _translateRdkafkaBrokerStats(brokerStats, globalLabels) {
    const brokerLabels = {...globalLabels, name: brokerStats.name
    , nodeid: brokerStats.nodeid}

    for (const key of Object.keys(brokerStats)) {
      switch (key) {
        case 'name':
        case 'nodeid':
          // Ignore: these are in brokerLabels already
          break
        case 'int_latency':
        case 'rtt':
        case 'throttle':
          this._translateRdKafkaBrokerWindowStats(key, brokerStats[key], brokerLabels)
          break
        case 'toppars':
          for (const topparName of Object.keys(brokerStats[key])) {
            this._translateRdKafkaBrokerTopparsStats(brokerStats[key][topparName], brokerLabels)
          }
          break
        case 'state':
          this._translateRdkafkaStat(`broker_${key}`, brokerStats[key], brokerLabels, (state) => {
            const value = BROKER_STATES.indexOf(state)
			/* istanbul ignore if */
            if (value === -1) {
              logger.warn(`Cannot map rdkafka broker state '${state}' to prometheus value`)
            }
            return value
          })
          break
        default:
          this._translateRdkafkaStat(`broker_${key}`, brokerStats[key], brokerLabels)
          break
      }
    }
  }

  _translateRdkafkaTopicPartitionStats(statKey, topicPartitionStats, topicLabels) {
    const topicPartitionLabels = {...topicLabels, partition: topicPartitionStats.partition}
    for (const key of Object.keys(topicPartitionStats)) {
      switch (key) {
        case 'partition':
          // Ignore: Part of the topic partition labels
          break
        case 'desired':
        case 'unknown':
          this._translateRdkafkaStat(`topic_partition_${key}`, topicPartitionStats[key], topicPartitionLabels, (state) => {
			/* istanbul ignore next */
            return state ? 1 : 0
          })
          break
        case 'fetch_state':
          this._translateRdkafkaStat(`topic_partition_${key}`, topicPartitionStats[key], topicPartitionLabels, (state) => {
            const value = FETCH_STATES.indexOf(state)
			/* istanbul ignore if */
            if (value === -1) {
              logger.warn(`Cannot map rdkafka topic partition fetch state '${state}' to prometheus value`)
            }
            return value
          })
          break
        case 'commited_offset':
          // Ignore: see https://github.com/edenhill/librdkafka/issues/80
          break
        default:
          this._translateRdkafkaStat(`topic_partition_${key}`, topicPartitionStats[key], topicPartitionLabels)
          break
      }
    }
  }

  _translateRdkafkaTopicStats(topicStats, globalLabels) {
    const topicLabels = {...globalLabels, topic: topicStats.topic}
    for (const key of Object.keys(topicStats)) {
      switch (key) {
        case 'topic':
          // Ignore: Part of the topic labels
          break
        case 'partitions':
          for (const topicPartitionId of Object.keys(topicStats[key])) {
            this._translateRdkafkaTopicPartitionStats(key, topicStats[key][topicPartitionId], topicLabels)
          }
          break
        default:
          this._translateRdkafkaStat(`topic_${key}`, topicStats[key], topicLabels)
          break
      }
    }
  }

  _translateRdkafkaCgrpStats(cgrpStats, globalLabels) {
    const cgrpLabels = {...globalLabels}
    for (const key of Object.keys(cgrpStats)) {
      this._translateRdkafkaStat(`cgrp_${key}`, cgrpStats[key], cgrpLabels)
    }
  }

  _translateRdkafkaStats(stats) {
    const globalLabels = {...this.extraLabels, handle: stats.name
    , type: stats.type}
    for (const key of Object.keys(stats)) {
      switch (key) {
        case 'name':
        case 'type':
          // Ignore: these are global labels
          break
        case 'brokers':
          for (const brokerName of Object.keys(stats[key])) {
            this._translateRdkafkaBrokerStats(stats[key][brokerName], globalLabels)
          }
          break
        case 'topics':
          for (const topicName of Object.keys(stats[key])) {
            this._translateRdkafkaTopicStats(stats[key][topicName], globalLabels)
          }
          break
        case 'cgrp':
          this._translateRdkafkaCgrpStats(stats[key], globalLabels)
          break
        default:
          this._translateRdkafkaStat(key, stats[key], globalLabels)
          break
      }
    }
  }

  /**
  * "Observe" the given statistics
  *
  * This internally translates the statistics into many prometheus metrics.
  *
  * @param {object} stats rdkafka raw statistics
  * @return {void}
  */
  observe(stats) {
    this._translateRdkafkaStats(stats)
  }
}

module.exports = RdkafkaStats
