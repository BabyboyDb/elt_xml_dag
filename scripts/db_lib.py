import psycopg2
import sys
from psycopg2 import sql
from psycopg2 import extras

# Функция создает коннектор к базе данных postgresql
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


# clear the table
def truncate_table(conn, tbl_name):
    with conn.cursor() as cur:
        cur.execute("truncate table {} ;".format(tbl_name))
    conn.commit()


# write dataframe to table
def insert2Table(conn, df, table_name, check_columns):
    list_columns_name = list(df.columns)
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, list_columns_name)),
        sql.SQL(', ').join(sql.Placeholder() * len(list_columns_name)),
        sql.Identifier(check_columns))

    num = 0
    for rec in df.values:
        coumns_values = rec.tolist()
        num += 1
        try:
            with conn.cursor() as cur:
                cur.execute(query, tuple(coumns_values))
            conn.commit()
        except psycopg.DatabaseError as err:
            print(err)
            print("Ошибка в строке {}!".format(num-1))
            print(coumns_values)
            if conn: conn.rollback()


def insert2Table_batch(conn,df,table_name,check_columns):
    list_columns_name=list(df.columns)
    query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, list_columns_name)),
        sql.Identifier(check_columns))

    insert_command = query.as_string(conn)
    records = tuple(df.itertuples(index=False, name=None))

    with conn.cursor() as cursor:
        extras.execute_values(cursor, insert_command,( (rec) for rec in records) )


# The function returns a list of field names in the table


def get_columns_names(conn, table_name):
    conn.autocommit = True

    sql_command = "SELECT * FROM {} LIMIT 1".format(table_name)
    column_names = []
    with conn.cursor() as cur:
        cur.execute(sql_command)
        column_names = [desc[0] for desc in cur.description]
    return column_names

# The function returns a list of field names in the table
def get_tbl_columns_name_and_size(conn, table_name):
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.column_name  , c.character_maximum_length  FROM INFORMATION_SCHEMA.columns c WHERE \
             c.table_name = '{}' and c.table_schema ='public';".format(table_name))
        column_names = [row for row in cur]
    return column_names

# The function returns a list of field names in a table of only one type. (fldtype parameter)


def get_tbl_columns_names_only_type(conn, table_name, fldtype):
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.column_name  FROM INFORMATION_SCHEMA.columns c WHERE \
             c.table_name = '{}' and c.table_schema ='public' and c.data_type = '{}';".format(table_name, fldtype))
        column_names = [row[0] for row in cur]
    return column_names


# select the columns of the first list that are not in the second
def get_list_columns4drop(df_cols, tbl_cols):
    drop_list = []
    for name in df_cols:
        if name not in tbl_cols:
            drop_list.append(name)
    return drop_list

# select the columns of the first list (frame) that are in the second (database table)
def get_list_columns4type(df_cols, tbl_cols):
    find_list = []
    for name in df_cols:
        if name in tbl_cols:
            find_list.append(name)
    return find_list
