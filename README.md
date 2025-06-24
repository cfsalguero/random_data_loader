# random_data_loader

random_data_loader is a Go program that generates and loads random data into a specified table in a MySQL or PostgreSQL database. It is useful for testing, development, and benchmarking by quickly populating tables with synthetic data that matches the table's schema.

## Features
- Supports MySQL and PostgreSQL databases
- Automatically parses table structure and generates appropriate random data
- Batch inserts for performance
- Parallel data loading
- Configurable via command line parameters
- Structured logging

## Usage

Build the project:

```sh
make build
```

Start the required database environment (if using Docker):

```sh
make env-up
```

Run the program:

```sh
./bin/random_data_loader [flags]
```

## Command Line Parameters

The following command line parameters are available:

| Parameter      | Type    | Default                                      | Description                                      |
|---------------|---------|----------------------------------------------|--------------------------------------------------|
| --type        | string  | mysql                                        | Database type: `mysql` or `postgres`             |
| --dsn         | string  | root=root@tcp(localhost:3306)/my_database    | Database connection string (DSN)                 |
| --schema      | string  | my_database                                  | Database schema name                             |
| --table       | string  | test_table                                   | Table name to parse and load data into           |
| --rows        | int     | 10                                           | Number of rows to generate and insert            |
| --parallel    | int     | 1                                            | Number of parallel processes to use              |
| --batch       | int     | 1000                                         | Batch size for inserting data                    |
| --log         | string  | info                                         | Log level: `debug`, `info`, `warn`, or `error`   |

### Example

Populate 1000 rows into a PostgreSQL table using 4 parallel workers and a batch size of 500:

```sh
./bin/random_data_loader --type=postgres --dsn="postgres://user:pass@localhost:5432/mydb?sslmode=disable" --schema=mydb --table=users --rows=1000 --parallel=4 --batch=500 --log=info
```

## Cleaning Up

To stop and remove Docker containers:

```sh
make env-down
```

## License

MIT License
