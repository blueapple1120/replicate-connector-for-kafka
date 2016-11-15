Configuration Options
---------------------

``tasks.max``
  Maximum number of tasks to start for processing PLOGs.

  * Type: string
  * Importance: high
  * Default: 4``

``topic.prefix``
  Prefix for all topic names.

  * Type: string
  * Default: REP-
  * Importance: high``

``plog.location.uri``
  Replicate PLOG location URI, output of Replicate MINE.

  * Type: string
  * Default: file:/home/oracle/ktest/mine
  * Importance: high``

``plog.data.flush.size``
  LCRs to cache before flushing, for connector this is the batch size, choose this value according to transactional volume, for high throughput to kafka the default value may suffic, for low or sporadic volume lower this value, eg. for testing use 1 which will not use cache and emit every record immediately.

  * Type: string
  * Default: 1000
  * Importance: high``

``plog.interval.time.ms``
  Time in milliseconds for one wait interval, used by scans and health check.

  * Type: string
  * Default: 500
  * Importance: high``

``plog.scan.interval.count``
  Number of intervals between scans, eg. 5 x 0.5s = 2.5s scan wait time.

  * Type: string
  * Default: 5
  * Importance: high``

``plog.health.check.interval``
  Number of intervals between health checks, these are used when initially waiting for MINE to produce PLOGs, eg. 10 * 0.5s = 5.0s.

  * Type: string
  * Default: 10
  * Importance: high``

``plog.scan.offline.interval``
  Default number of health check scans to decide whether or not replicate is offline, this is used as time out value. NOTE for testing use 1, i.e. quit after first health check 1 * 10 * 0.5s = 5s where 10 is plog.health.check.interval value and 0.5s is plog.interval.time.ms value.

  * Type: string
  * Default: 1000
  * Importance: high``

``topic.name.transaction.info``
  Topic name for transaction meta data stream.

  * Type: string
  * Default: TX.META
  * Importance: high``

``plog.global.scn.cold.start``
  Global SCN when to start loading data during cold start.

  * Type: string
  * Default: 0
  * Importance: high``
