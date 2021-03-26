# redis-cdc-integration-test

## Download

Download the [latest release](https://github.com/RedisLabs-Field-Engineering/redis-cdc-integration-test/releases) e.g. ```wget https://github.com/RedisLabs-Field-Engineering/redis-cdc-integration-test/releases/download/v1.0/redis-cdc-integration-test-1.0-SNAPSHOT.tar.gz``` and untar (tar -xvf redis-cdc-integration-test-1.0-SNAPSHOT.tar.gz) the redis-cdc-integration-test-1.0-SNAPSHOT.tar.gz archive.

All the contents would be extracted under redis-cdc-integration-test directory

Contents of redis-cdc-integration-test
<br>•	bin – contains script files
<br>•	lib – contains java libraries
<br>•	config – contains sample config and data files for integration tests

## Launch

`redis-cdc-integration-test/bin$ ./start.sh`

```bash
Usage: redis-cdc-integration-test [OPTIONS] [COMMAND]
Integration test framework for redis-cdc.
  -h, --help   Show this help message and exit.
Commands:
  loadsqldata        Load data into source table using sql insert statements.
  loadsqlandcompare  Load source table with sql inserts and compare them with target JSON objects.
  loadcsvdata        Load CSV data to source table.
  loadcsvandcompare  Load CSV data to source and print live comparisons.
```
