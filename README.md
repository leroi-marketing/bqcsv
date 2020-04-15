# `bqcsv` BigQuery flat CSV exporter

### Installing

```sh
pip install --upgrade git+https://github.com/leroi-marketing/bqcsv
```

### Usage

Simples usage example is piping a query to STDIN. The CSV output is produced in STDOUT:

```sh
cat data/query.sql | bqcsv > out.csv
```

For a more detailed usage, see `bqcsv --help`
