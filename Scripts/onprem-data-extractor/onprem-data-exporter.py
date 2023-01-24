import cx_Oracle
import pandas as pd
import datetime

currentDateTime = str(datetime.date.today())

def write_csv(col_name, data, table_name):
    """writes tables data as parquet to s3 using table data and table_name as input"""
    try:
        df = pd.DataFrame(data, columns=col_name)
        s3_path = f"s3://parquets-del-vij/tables/{currentDateTime}/" + table_name.upper() + ".parquet"
        # additional requirements: fastparquet, fsspec, s3fs to write dataframe as parquets to s3
        df.to_parquet(s3_path)
        print("written {} table data to S3 is successful".format(table_name))
    except Exception as e:
        print("err in writing", e)


def read_tables(tables):
    """reads tables from Database taking list of tables as input"""
    conn = cx_Oracle.connect('system/vijay@localhost')
    cur = conn.cursor()
    try:
        with conn:
            for table in tables:
                query = """SELECT * FROM {}""".format(table)
                cur.execute(query)
                data = cur.fetchall()
                col_name = [i[0].lower() for i in cur.description]
                write_csv(col_name, data, table)
    except Exception as e:
        print(f"err in reading {e}")


def main():
    tables = ['EMP', 'DEPT', 'SALGRADE', 'DUMMY']
    read_tables(tables)


if __name__ == '__main__':
    main()
