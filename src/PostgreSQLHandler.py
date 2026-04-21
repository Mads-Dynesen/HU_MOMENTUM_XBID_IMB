import psycopg2
import configparser
from io import StringIO
import datetime


def _get_params(table_name: str):
    config = configparser.ConfigParser()
    config.read("configfile.ini")
    params = config[table_name]
    return params


def connect_db(conn_params_dic):
    conn, cursor = None, None
    try:
        # print("Establishing connection to PostgreSQL")
        conn = psycopg2.connect(**conn_params_dic)
        cursor = conn.cursor()

        # print("*** Success ***")
    except Exception as err:
        print(err)
        # Nulling connection
        conn = None
        cursor = None
    return conn, cursor


def insert_dict(
    column_names: list, table_name: str, conflict_str: str, values: list
) -> None:
    """
    Used to insert a dict into our database. Currently uses the dev database. Requires a ini file with the table name.
    :param column_names: list of column names
    :param table_name: Name of the table to be inserted into
    :param conflict_str: The values to look at for regarding conflicts. Must be in the form of (name, name1)
    :param values: A list of the values to put into the database.
    """
    conn, cursor = None, None
    try:
        config = configparser.ConfigParser()
        config.read("configfile.ini")
        params = config[table_name]
        conn, cursor = connect_db(params)

        sql_string = f"INSERT INTO {table_name} "
        sql_string += "(" + ", ".join(column_names) + ")\n VALUES "

        sql_string += "(" + ", ".join(values) + "),\n"
        sql_string = sql_string[:-2]
        sql_string += " \nON CONFLICT " + conflict_str + " DO NOTHING;"
        # print(sql_string)
        cursor.execute(sql_string)
        conn.commit()

    except Exception as error:
        print(
            datetime.datetime.now(),
            "Failed insert into {} {}".format(table_name, error),
        )
    finally:
        if conn:
            cursor.close()
            conn.close()
            # print("Closing Connection")


def insert_pandas_into_db(data, table_name: str, eic=False):
    conn, cursor = None, None
    insert_string = ""
    try:
        config = configparser.ConfigParser()
        config.read("configfile.ini")
        params = config[table_name]
        conn, cursor = connect_db(params)

        start_date_time = data.datetime_begin.min()
        end_date_time = data.datetime_begin.max()

        columns = data.columns
        sql_string = generate_sql(columns, eic=eic, table_name=table_name)
        # variable_col = [c for c in columns if c != "identifier"]

        # update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in variable_col])
        buffer = StringIO()
        data.to_csv(buffer, header=False, index=False)
        buffer.seek(0)

        temp_table_name = "temp_" + table_name

        create_temp_table = f"CREATE TEMP TABLE {temp_table_name} " + sql_string.get(
            "sql_temp"
        )
        # create_temp_table = f"CREATE TABLE {temp_table_name} " + sql_string.get('sql_temp')
        cursor.execute(create_temp_table)

        cursor.copy_from(buffer, temp_table_name, sep=",")

        insert_string = f"""
        INSERT INTO {table_name}({", ".join(columns)})
        select {sql_string.get("select_str")}
        FROM {temp_table_name} tn
        {sql_string.get("join_str")}
        where not exists (
        select *
        from (
            select *,
            RANK () OVER (PARTITION BY {", ".join([col for col in columns if (col != "fact_value") & (col != "row_insert_datetime") & (col != "row_update_datetime") & (col != "datetime_forecast") & (col != "forecast_datetime")])} ORDER BY row_insert_datetime DESC) row_rank
            from {table_name}
            where {table_name}.datetime_begin between '{start_date_time}' and '{end_date_time}'
            ) a
            where a.row_rank = 1
            {sql_string.get("sql_exists_string")}
        )
        """
        # print(insert_string)
        cursor.execute(insert_string)
        conn.commit()

        print(
            cursor.rowcount,
            "Inserted successfully into table:",
            table_name,
            f"data_len: {len(data.index)}",
        )
    except Exception as error:
        print(
            datetime.datetime.now(),
            __name__,
            "Failed insert into {} {}".format(table_name, error),
        )
        # import helperfun
        # helperfun.print_pandas(data[data['dim_power_price_area_sk_from'] == '10YFR-RTE------C'])
        print(f'{data["dim_power_price_area_sk_from"].unique()= }\n\n')
        print(f'{data["dim_power_price_area_sk_to"].unique()= }')
        print(insert_string)
    finally:
        if conn:
            cursor.close()
            conn.close()
            # print("Closing connection")


def extract_SQL(table_name, sql) -> list:
    conn, cursor = None, None
    try:
        config = configparser.ConfigParser()
        config.read("configfile.ini")
        params = config[table_name]
        conn, cursor = connect_db(params)

        cursor.execute(sql)

        return cursor.fetchall()
    except Exception as error:
        print("Failed insert into {} {}".format(table_name, error))
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Closing connection")


def generate_sql(col_names, table_name: str, type=None, eic=False):
    dim_names = [name for name in col_names if "dim" in name]
    fact_names = [name for name in col_names if "dim" not in name]

    result = {}
    temp_table_generate = []
    for x in col_names:
        if type is not None and x in type:
            specified = type.get(x)
            temp_table_generate.append(specified)
        elif "datetime" in x:
            temp_table_generate.append("TIMESTAMP WITH TIME ZONE")
        elif "dim_" in x:
            temp_table_generate.append("TEXT")
        elif "block_id" in x:
            temp_table_generate.append("NUMERIC")
        elif "fact_value" in x:
            if table_name in [
                "fact_power_flow_forecast",
                "fact_power_flow",
                "fact_day_ahead",
                "fact_production_forecast",
                "fact_production_actual",
                "fact_consumption_actual",
            ]:
                temp_table_generate.append("REAL")
            else:
                temp_table_generate.append("NUMERIC")

    sql_temp_string = "(%s)" % ", ".join(
        [a + " " + b for a, b in zip(col_names, temp_table_generate)]
    )

    # creating 'left join' string
    join_shorthand = []
    for x in col_names:
        words = x.split("_")
        letters = [word[0] for word in words]
        join_shorthand.append("".join(letters))

    sql_join_string = ""
    short_dim = [x for x in join_shorthand if "d" in x[0]]
    for x, y in zip(dim_names, short_dim):
        dim_table = x.split("_sk")[0]
        res = "left join "
        res += dim_table
        res += " " + y + " on "
        if dim_table in ("dim_power_price_area", "dim_power_grid") and eic:
            res += y + ".eic_code" + " = tn." + x + "\n"
        else:
            res += y + "." + dim_table.split("dim_")[1] + " = tn." + x + "\n"
        sql_join_string += res

    sql_exists_string = ""
    short_dim = [x for x in join_shorthand if "d" in x[0]]
    for x, y in zip(dim_names, short_dim):
        if x in ["dim_power_price_area_sk_to", "dim_power_price_area_sk_from"]:
            res = "and " + y + "." + x.rsplit("_", 1)[0]
        else:
            res = "and " + y + "." + x
        res += " = a." + x + "\n"
        sql_exists_string += res
    # for x, y in zip(dim_names, short_dim):
    #     res = 'and ' + y + '.' + x
    #     res += ' = a.' + x + '\n'
    #     sql_exists_string += res

    for x in list(set(col_names) - set(dim_names)):
        if "row" in x or x == "datetime_forecast" or x == "forecast_datetime":
            continue
        sql_exists_string += "and tn." + x + " = a." + x + "\n"

    sql_select_str = ""
    for name, short in zip(col_names, join_shorthand):
        if name in fact_names:
            sql_select_str += "tn." + name
        else:
            if name in ["dim_power_price_area_sk_to", "dim_power_price_area_sk_from"]:
                sql_select_str += short + "." + name.rsplit("_", 1)[0]
            else:
                sql_select_str += short + "." + name
        sql_select_str += ", "
    # for name, short in zip(col_names, join_shorthand):
    #     if name in fact_names:
    #         sql_select_str += 'tn.' + name
    #     else:
    #         sql_select_str += short + '.' + name
    #     sql_select_str += ', '

    result["sql_temp"] = sql_temp_string
    result["name_abbr"] = join_shorthand
    result["join_str"] = sql_join_string
    result["select_str"] = sql_select_str[:-2]
    result["sql_exists_string"] = sql_exists_string
    return result


def insert_no_dim(data, table_name: str, conflict_str: str = None):
    conn, cursor = None, None
    try:
        config = configparser.ConfigParser()
        config.read("configfile.ini")
        params = config[table_name]
        conn, cursor = connect_db(params)

        columns = data.columns

        buffer = StringIO()
        data.to_csv(buffer, header=False, index=False)
        buffer.seek(0)
        temp_table_name = "temp_" + table_name

        # Columns you want to exclude
        exclude_cols = ["row_insert_datetime", "row_update_datetime"]

        cursor.execute(f"""
            CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING ALL);
        """)

        for col in exclude_cols:
            cursor.execute(f'ALTER TABLE {temp_table_name} DROP COLUMN "{col}";')

        cursor.copy_from(buffer, temp_table_name, sep=",", null="None")

        cursor.execute(f"""
            INSERT INTO {table_name}({", ".join(columns)})
            SELECT {", ".join(columns)} FROM {temp_table_name};
        """)

        conn.commit()
        print(cursor.rowcount, "Inserted successfully into table:", table_name)

    except Exception as error:
        print(
            datetime.datetime.now(),
            "Failed insert into {} {}".format(table_name, error),
        )
        return
    finally:
        if conn:
            cursor.close()
            conn.close()
            # print("Closing connection")

