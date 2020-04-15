import sys, os, csv, json
from functools import namedtuple
from argparse import ArgumentParser
from typing import Any, List, Dict, Iterator
from google.cloud import bigquery
from google.api_core import exceptions
from google.cloud.bigquery.job import QueryJob


Field = namedtuple("Field", ("name", "strname", "datatype"))

class Worker:
    def __init__(self, args):
        self.__args = args
        if self.__args.query:
            self.__stmt = self.__args.query
        elif self.__args.query_file:
            with open(self.__args.query_file, 'r') as fp:
                self.__stmt = fp.read()
        else:
            self.__stmt = sys.stdin.read()

        credentials_path = os.path.abspath(self.__args.auth)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    def __enter__(self):
        self.__client = bigquery.Client()
        return self

    def __exit__(self, *args):
        pass

    def get_fields(self, schema: List, path: List[str] = []) -> Iterator[Field]:
        """ Flattens the multi-level schema and returns a list of all the fields
        """
        for column in schema:
            name = path + [column.name]
            if self.__args.nf2 and column.field_type == 'RECORD':
                yield from self.get_fields(column.fields, name)
            else:
                yield Field(name, '.'.join(name), column.field_type)

    def get_value(self, raw_row: Dict, field: Field) -> Any:
        """ Get a specific field value from a BigQuery data row
        """
        value = raw_row
        for name_part in field.name:
            if hasattr(value, 'get'):
                value = value.get(name_part)
            else:
                return None
            if value is None:
                return None
        if not self.__args.nf2 and field.datatype == 'RECORD':
            value = json.dumps(value)
        return value

    def get_row(self, raw_row: Dict, fields: List[Field]) -> List[Any]:
        """ Get a list of scalar values from a BigQuery row
        """
        row = []
        for field in fields:
            row.append(self.get_value(raw_row, field))
        return row

    def work(self):
        result: QueryJob = self.__client.query(self.__stmt).result()
        fields = list(self.get_fields(result.schema))

        # Write schema information to a file if requested
        if self.__args.schema:
            os.makedirs(self.__args.schema, exist_ok=True)
            filename = os.path.realpath(self.__args.schema) + '/schema.json'
            with open(filename, 'w') as fp:
                json.dump([
                    {"name": f.strname, "type": f.datatype} for f in fields
                ], fp)

        # Write the data to the output
        out_dest = sys.stdout
        need_to_close = False
        try:
            if self.__args.out:
                out_dest = open(self.__args.out, 'w')
                need_to_close = True

            writer = csv.writer(out_dest)
            writer.writerow((f.strname for f in fields))

            writer.writerows(self.get_row(row, fields) for row in result)
        finally:
            if need_to_close:
                out_dest.close()


def setup_arguments():
    parser = ArgumentParser(description="Outputs bigquery results as CSV")

    parser.add_argument(
        '-a',
        '--auth',
        dest='auth',
        help="Path to bigquery json-formatted authentication config. Default: auth/auth.json",
        type=str,
        default='auth/auth.json'
    )

    parser.add_argument(
        '-o',
        '--out',
        dest='out',
        help="Output file destination. Defaults to stdout",
        type=str
    )

    parser.add_argument(
        '-s',
        '--schema-out',
        dest='schema',
        help="Path of output schema details",
        type=str
    )

    parser.add_argument(
        '--nf2',
        dest='nf2',
        help="Produce normalized schema and data for muptiple tables from underlying structures, as opposed to saving"
            " them as JSON text.",
        action='store_true',
        default=False
    )

    group = parser.add_mutually_exclusive_group(required=False)

    group.add_argument(
        '-q',
        '--query',
        dest='query',
        help="SQL query to execute",
        type=str,
    )

    group.add_argument(
        '-f',
        '--query-file',
        dest='query_file',
        help="SQL query file",
        type=str
    )

    return parser.parse_args()


def main():
    args = setup_arguments()
    with Worker(args) as worker:
        worker.work()

if __name__=='__main__':
    main()
