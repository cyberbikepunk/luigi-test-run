from os import getenv

from boto.s3.key import Key
from boto.s3.connection import S3Connection
from luigi import Task, run, IntParameter
from luigi.s3 import S3Client, S3Target
from pandas import DataFrame
import numpy as np


ALPHABET = list(map(chr, range(97, 123)))
ACCESS_KEY = getenv('AWS_ACCESS_KEY_ID')
ACCESS_SECRET = getenv('AWS_SECRET_ACCESS_KEY')
BUCKET = 'mybucketloictest'


class CreateRandomData(Task):
    nrows = IntParameter(default=20)
    ncolumns = IntParameter(default=4)
    csv_string = None
    s3_client = S3Client(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=ACCESS_SECRET)

    def requires(self):
        return []

    def output(self):
        s3_filepath = 's3://' + BUCKET + '/random_numbers.csv'
        return S3Target(s3_filepath)

    def run(self):
        df = DataFrame(np.random.rand(self.nrows, self.ncolumns), columns=ALPHABET[0:self.ncolumns])
        df.index.name = 'index'
        output = df.to_string()

        conn = S3Connection(ACCESS_KEY, ACCESS_SECRET)
        bucket = conn.get_bucket(BUCKET)
        file = Key(bucket)
        file.key = 'random_numbers.csv'
        file.set_contents_from_string(output)

if __name__ == '__main__':
    run()
