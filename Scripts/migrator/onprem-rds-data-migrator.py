import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import psycopg2
import threading

sc = SparkContext()
glueContext = GlueContext(sc)
LOGGER = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_json'])

response = json.loads(args['config_json'])
LOGGER.info("response {}".format(response))
key = response.get("key")
s_count = key.count('/')
key_name = "/".join(key.split("/")[0:s_count])
source_path = "s3://" + response.get("bucket_name") + "/" + key_name
writer_secret = response.get("Writer_Secret")
bucket_name = response.get("bucket_name")
region = "us-east-1"
dbUser = response.get("user")
dbPass = response.get("password")
dbName = response.get("dbName")
dbPort = response.get("port")
dbHost = response.get("host")
jdbcUrl = "jdbc:postgresql://{}:{}/{}".format(dbHost, dbPort, dbName)
main_tables_count = {}
tables = ['EMP', 'DEPT', 'SALGRADE', 'DUMMY']

water_marked_dict = {"EMP": False, "DEPT": False, "SALGRADE": False, "DUMMY": False}
table_maximum_data_count_information = {"EMP": 14, "DEPT": 4, "SALGRADE": 5, "DUMMY": 1}

insert_query_stmt_dict = {
    "EMP": 'insert into office_main."EMP" (empno, ename, job, mgr, hiredate, sal, comm, deptno) values (%s,%s,%s,%s,%s,%s,%s,%s)',
    "DEPT": 'insert into office_main."DEPT" (deptno, dname, loc) values (%s,%s,%s)',
    "SALGRADE": 'insert into office_main."SALGRADE" (grade, losal, hisal) values (%s,%s,%s)',
    "DUMMY": 'insert into office_main."DUMMY" (dummy) values (%s)'
    }
delete_query_stmt_dict = {"EMP": """delete from office_main."EMP" where  empno='{}'""",
                          "DEPT": """delete from office_main."DEPT" where  deptno='{}'""",
                          "SALGRADE": """delete from office_main."SALGRADE" where  grade='{}'""",
                          "DUMMY": """delete from office_main."DUMMY" where  dummy='{}'"""
                          }
get_distinct_record = {"EMP": 'SELECT DISTINCT empno from office_stage."EMP"',
                       "DEPT": 'SELECT DISTINCT deptno from office_stage."DEPT"',
                       "SALGRADE": 'SELECT DISTINCT grade from office_stage."SALGRADE"',
                       "DUMMY": 'SELECT DISTINCT dummy from office_stage."DUMMY"'
                       }
get_all_records = {
    "EMP": """select * from office_stage."EMP" where empno='{}'""",
    "DEPT": """select * from office_stage."DEPT" where deptno='{}'""",
    "SALGRADE": """select * from office_stage."SALGRADE" where grade='{}'""",
    "DUMMY": """select * from office_stage."DUMMY" where dummy='{}'"""
}


def get_dynamic_frame(path):
    df = glueContext.create_dynamic_frame.from_options(connection_type="s3", format="parquet",
                                                       connection_options={"paths": [path], "recurse": True},
                                                       transformation_ctx="df")

    return df


def table_data_count(dataframe, key):
    if dataframe.count() >= table_maximum_data_count_information[key]:
        return True
    else:
        return False


def main():
    try:
        for table in tables:
            filePath = source_path + "/" + table + ".parquet"
            df = get_dynamic_frame(filePath)
            dbTable = "office_stage." + table
            if table_data_count(df.toDF(), table):
                writeDataRds(df, dbTable)
                LOGGER.info("data writing into staging table {} is completed".format(dbTable))
                water_marked_dict[table] = True
    except Exception as e:
        LOGGER.info("data writing failed into staging table {}, {}".format(table, e))
        raise


def writeDataRds(datasource, dbTable):
    try:
        DataSink1 = glueContext.write_dynamic_frame.from_options(frame=datasource,
                                                                 connection_options={"url": jdbcUrl, "user": dbUser,
                                                                                     "password": dbPass,
                                                                                     "dbtable": dbTable},
                                                                 connection_type='postgresql')
    except Exception as e:
        LOGGER.info("not able to write the data {}".format(e))


def clear_db():
    try:
        connection = psycopg2.connect(user=dbUser, password=dbPass, host=dbHost, port=dbPort, database=dbName)
        LOGGER.info("conn {}".format(connection))
        cursor = connection.cursor()
        for table in tables:
            query = f'DELETE FROM office_stage."{table}"'
            cursor.execute(query)
            connection.commit()
        LOGGER.info("Data in tables cleared successfully")
        connection.close()
    except Exception as e:
        LOGGER.info("failed to clear data in tables. {}".format(e))


def get_data_from_db(table_name):
    try:
        connection = psycopg2.connect(user=dbUser, password=dbPass, host=dbHost, port=dbPort, database=dbName)
        cursor = connection.cursor()
        sql_query = get_distinct_record[table_name]
        cursor.execute(sql_query)
        data = [row for row in cursor.fetchall()]
        connection.close()
    except Exception as e:
        LOGGER.error("Not able to connect with DB Reader instance and error {}".format(e))
    return data


def insertData(table_name):
    LOGGER.info("water marked value for {} is {}".format(table_name, water_marked_dict[table_name]))
    insert_stmt = insert_query_stmt_dict[table_name]
    delete_stmt = delete_query_stmt_dict[table_name]
    if water_marked_dict[table_name]:
        try:
            connection2 = psycopg2.connect(user=dbUser, password=dbPass, host=dbHost, port=dbPort, database=dbName)
            data = get_data_from_db(table_name)
            idx = 0
            total = 0
            row_count = 0
            with connection2:
                with connection2.cursor() as curs2:
                    LOGGER.info("started writing into table {}".format(table_name))
                    for p_num in data:
                        delete_stmt_1 = delete_stmt.format(p_num[0])
                        curs2.execute(delete_stmt_1)
                        query = get_all_records[table_name]
                        query1 = query.format(p_num[0])
                        curs2.execute(query1)
                        rows = curs2.fetchall()
                        curs2.executemany(insert_stmt, rows)
                        idx += curs2.rowcount
                        row_count += 1
                        if row_count == len(data):
                            connection2.commit()
                            total += idx
                            LOGGER.info("commited {0} records in table {1}".format(total, table_name))
                            idx = 0
                    LOGGER.info("data wrting is completed in table {}".format(table_name))
            main_tables_count[table_name] = total
            connection2.close()
            LOGGER.info("database writer instance connection is closed")
            LOGGER.info("completed execution of  table {}".format(table_name))
        except Exception as e:
            LOGGER.error("Not able to connect DB writer Instance and error {}".format(e))
    else:
        LOGGER.info("we are not migrating data due to water check marked failed for table {}".format(table_name))

    return insert_stmt


def migrate():
    t1 = threading.Thread(target=insertData, args=('EMP',))
    t2 = threading.Thread(target=insertData, args=('DEPT',))
    t3 = threading.Thread(target=insertData, args=('SALGRADE',))
    t4 = threading.Thread(target=insertData, args=('DUMMY',))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    if len(main_tables_count) > 0:
        LOGGER.info("Data migration from staging to main successful")
    else:
        LOGGER.error("Data migration to main from staging failed.")


if __name__ == '__main__':
    clear_db()
    main()
    migrate()
