import xml.etree.ElementTree as Et
from datetime import datetime
from tkinter import filedialog, Tk

import cx_Oracle
import pandas as pd


def get_connection(username, password, db):
    """Get oracle connection information for a1 or a2 database."""
    if db == "a1":
        hostname = "127.0.0.1"
        port = "8080"
        service_name = "abcd"
    else:
        hostname = r"127.0.0.2"
        port = "9090"
        service_name = "xyz"
    dsn_tns = cx_Oracle.makedsn(
        hostname, port, service_name=service_name
    )  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(
        user=r"{user}".format(user=username),
        password=r"{pwd}".format(pwd=password),
        dsn=dsn_tns,
    )
    return conn


def get_output_file_path(title_name):
    """Open file explorer window and ask user to choose a output directory and file name to save the output."""
    root = Tk()
    root.filename = filedialog.asksaveasfilename(
        title=title_name, filetypes=(("CSV Files", "*.csv"),)
    )
    root.destroy()
    return root.filename


def generate_sources(source: list):
    """Generate source csv file along with relevant data."""
    source_data = [
        {
            "Tables": table[0],
            "Type": table[1],
            "Comments": table[2],
        }
        for table in source
    ]
    df = pd.DataFrame(source_data)
    output_path = get_output_file_path("Choose output file path and name") + ".csv"
    df.to_csv(output_path, index=False)


def read_xdm(title_name: str):
    """Open file explorer window and ask user to choose a XDM file."""
    root = Tk()
    root.filename = filedialog.askopenfilename(
        title=title_name, filetypes=(("XDM Files", "*.xdm"),)
    )
    root.destroy()
    return root.filename


def read_xdm_and_get_sql_map_with_headers(xdm, inputs) -> dict:
    """Read the XDM file to read SQL queries with their names and generate source info accordingly."""
    tree = Et.parse(xdm)
    root = tree.getroot()

    ns = {
        "xmlns": "http://xmlns.oracle.com/oxp/xmlp",
        "xmlns:xdm": "http://xmlns.oracle.com/oxp/xmlp",
        "xmlns:xsd": "http://wwww.w3.org/2001/XMLSchema",
    }
    sql_with_header: dict = {}
    for dataSets in root.findall("xmlns:dataSets", ns):
        for dataSet in dataSets.findall("xmlns:dataSet", ns):
            sql_name = dataSet.get("name")
            for sql in dataSet.findall("xmlns:sql", ns):
                sql = sql.text
                sql_query = str(sql).strip()
                for inp in inputs:
                    parameter_name, parameter_value = inp.split("=")
                    sql_query = sql_query.replace(parameter_name, parameter_value)
                sql_with_header[sql_name] = sql_query
    return sql_with_header


def get_source_table_details(conn, query, name, database, source_list):
    """Get source table information from SQL queries."""
    c = conn.cursor()
    td_sql = (
        "select TABLE_NAME,OWNER,COMMENTS from all_tab_comments where owner in ('{owner}','SYS','SYSTEM') and "
        "TABLE_TYPE='TABLE'".format(owner=database)
    )
    table_details = c.execute(td_sql)
    table_info = []
    for td in table_details:
        table_info.append(td)
    tables = [item[0] for item in table_info]
    table_comments = {item[0]: item[2] for item in table_info}
    table_owners = {item[0]: item[1] for item in table_info}
    print("Getting source metadata for {sql_name}".format(sql_name=name))
    set_plan_query = (
        "EXPLAIN PLAN SET STATEMENT_ID = '{statement_id}' for {query}".format(
            statement_id=name, query=query
        )
    )
    c.execute(set_plan_query)
    get_plan_result_query = "Select OBJECT_NAME,OBJECT_TYPE from plan_table where STATEMENT_ID='{statement_id}'".format(
        statement_id=name
    )
    out = c.execute(get_plan_result_query)
    plan = []
    for row in out:
        plan.append(row)
    for i in plan:
        object_name = i[0]
        if (
            str(object_name).endswith("_PK")
            or str(object_name).endswith("_UK")
            or str(object_name).endswith("_FK")
        ):
            object_name = (
                str(object_name)
                .replace("_PK", "")
                .replace("_UK", "")
                .replace("_FK", "")
                .replace("00_IDX", "")
            )
        if object_name is not None and object_name in tables:
            table_type = "Utility Table"
            if table_owners[object_name] == database:
                table_type = "Application Source Table"
            if object_name not in [it[0] for it in source_list]:
                source_list.append(
                    [object_name, table_type, table_comments[object_name]]
                )


def main():
    xdm_path = read_xdm("Choose the XDM File")
    db_id = int(input("Choose the Database (Type 1 or 2).\n1 for a1\n2 for a2\n"))
    database = None
    if db_id == 1:
        database = "a1"
    elif db_id == 2:
        database = "a2"
    else:
        print("Not a Valid option.")
    if database is not None:
        db_credentials = str(
            input(
                "Enter Database username and password in the format of username,password.\n"
            )
        )
        username, password = db_credentials.split(",")
        connection = get_connection(username, password, database)
        input_parameters = input(
            "Enter all the input parameters in the format of parameterName1=parameterValue1,"
            "parameterName2=parameterValue2.\n"
        )
        input_list = str(input_parameters).split(",")
        start = datetime.now()
        sql_and_headers = read_xdm_and_get_sql_map_with_headers(xdm_path, input_list)
        td_list = []
        for header, sql in sql_and_headers.items():
            get_source_table_details(connection, sql, header, database, td_list)
        td_list.sort(key=lambda x: x[0])
        generate_sources(td_list)
        print("Sources tab generated for Reverse Engineering sheet.")
        stop = datetime.now()
        print("\nProgram execution Time: ", stop - start)
        connection.close()


if __name__ == "__main__":
    main()
