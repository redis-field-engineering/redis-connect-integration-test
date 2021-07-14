# redis-connect-integration-test

## Prerequisites

<br>• Redis Connect connector (e.g. SQL Server connector) is setup and running with the same source database, schema and table(s) as redis-connect-integration-test.
<br>• Please have Java Runtime Environment ([OpenJRE](https://openjdk.java.net/install/) or OracleJRE) installed prior to running redis-connect-integration-test.

## Download

Download the [latest release](https://github.com/RedisLabs-Field-Engineering/redis-connect-integration-test/releases) and untar (tar -xvf redis-connect-integration-test-1.0.0.tar.gz) the redis-connect-integration-test-1.0.0.tar.gz archive.

All the contents would be extracted under redis-connect-integration-test directory

Contents of redis-connect-integration-test
<br>•	bin – contains script files
<br>•	lib – contains java libraries
<br>•	config – contains sample config and data files for integration tests

## Launch

<br>[*nix OS](https://en.wikipedia.org/wiki/Unix-like):
`redis-connect-integration-test/bin$ ./start.sh`
<br>Windows OS:
`redis-connect-integration-test\bin> start.bat`

```bash
Usage: redis-connect-integration-test [OPTIONS] [COMMAND]
Integration test framework for redis-connect.
  -h, --help   Show this help message and exit.
Commands:
  compare            Compares Source and Target raw events in the same sequence as it occurs. 
  loadsqldata        Load data into source table using sql insert statements.
  loadsqlandcompare  Load source table with sql inserts and compare them with target JSON objects.
  loadcsvdata        Load CSV data to source table.
  loadcsvandcompare  Load CSV data to source and print live comparisons.
```
