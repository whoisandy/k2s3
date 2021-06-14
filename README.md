# k2s3

> Kafka to S3 Streamer

Stream JSON messages from Kafka to partitioned Parquet files stored on S3

## TODO:

- [x] Add Kafka consumer logic
- [ ] Enable partition columns as cli option
- [ ] Ability to enable/disable compaction
- [ ] Ability to alter table with new partitions dynamically
- [ ] Ability to include/exclude fields from JSON
- [ ] Convert batches to partitioned parquet tables
- [ ] Add/Update kafka offset checkpoint files along with parquet partitioned files
