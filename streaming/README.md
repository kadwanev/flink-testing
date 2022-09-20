
Build: mvn package

### List Jobs:
  `~/lib/flink/bin/flink list`

### Submit a job:
  `~/lib/flink/bin/flink run -d target/streaming-1.0-SNAPSHOT-AccountSumming.jar`

### Cancel a job with save: 
  `~/lib/flink/bin/flink cancel -s savepoints/account-summing 1ebf5954fac3dca02192581974cfa3dd`
> Cancelling job 1ebf5954fac3dca02192581974cfa3dd with savepoint to savepoints/account-summing.
>
> Cancelled job 1ebf5954fac3dca02192581974cfa3dd. Savepoint stored in file:/Users/nkadwa/lib/flink-1.13.2/savepoints/account-summing/savepoint-1ebf59-2ad07f741008.

### Resume a job: 
  `~/lib/flink/bin/flink run -d -s savepoints/account-summing/savepoint-1ebf59-2ad07f741008 target/streaming-1.0-SNAPSHOT-AccountSumming.jar`
  