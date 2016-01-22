#!/usr/bin/env python2

import glob
import psycopg2 as pg

if __name__ == '__main__':
    with pg.connect('dbname=daniel') as conn:
        with conn.cursor() as cur:
            for i in glob.glob('*.json'):
                cur.execute('INSERT INTO raw_data (data) VALUES (%s)',
                            (open(i).read(),))
