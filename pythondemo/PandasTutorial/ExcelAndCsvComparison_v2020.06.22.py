from tkinter import filedialog, Tk

import numpy as np
import pandas as pd
from prettytable import PrettyTable
import warnings

warnings.filterwarnings("ignore")


def get_format():
    """Ask users to choose an option for datetime format."""
    try:
        option = int(
            input(
                "Please choose date option to compare(Numbers only):"
                "\n1)2019-12-30 13:45:30.0003"
                "\n2)2019-12-30 13:45:30"
                "\n3)2019-12-30\n"
            )
        )

        if option == 1:
            return "%Y-%m-%d %H:%M:%S.%f"
        elif option == 2:
            return "%Y-%m-%d %H:%M:%S"
        elif option == 3:
            return "%Y-%m-%d"

    except ValueError:
        print("That's not an option!")
        print("Default to 2019-12-30\n")
        return "%Y-%m-%d"


def df_from_csv(file_path):
    """Return pandas dataframe from a CSV file."""
    return pd.read_csv(
        file_path, skipinitialspace=True, na_filter=False, index_col=False
    )


def transform_datetime(df, date_str_format):
    """Transform the datetime based on option selected by user."""
    for column_name in list(df.columns.values):
        if (
            ("_date" in str(column_name).lower())
            or ("_month" in str(column_name).lower())
            or ("_on" in str(column_name).lower())
        ):
            try:
                df[column_name] = pd.to_datetime(
                    df[column_name], infer_datetime_format=True, cache=True
                ).dt.strftime(date_str_format)
            except ValueError:
                pass
    return df


def get_difference_as_boolean_dataframe(left_df, right_df, index_column_name):
    """Returns a datframe with the list of column names which have different values for each row of 2nd file."""
    left_df = left_df.fillna("")
    right_df = right_df.fillna("")

    l_df = left_df.set_index(index_column_name)
    r_df = right_df.set_index(index_column_name)
    column_bool_list_df = l_df.isin(r_df)

    for column in column_bool_list_df:
        if column != "index":
            column_bool_list_df[column] = column_bool_list_df[column].map(
                {True: np.NaN, False: column}
            )
        else:
            column_bool_list_df[column] = column_bool_list_df[column]
    return column_bool_list_df


def get_location_of_difference(df):
    """Joins all column to single column."""
    df["Column_Name"] = df.apply(lambda x: ",".join(x.dropna().astype(str)), axis=1)
    result_df = df.iloc[:, -1:]
    return result_df


def output_to_csv(df, name):
    """Output dataframe to a CSV file."""
    df.to_csv(f"{name}.csv", index=False)


def check_column_equality(df1, df2, df1_cols, df2_cols):
    """Checks if number of columns and column names are same in both dataframe
    irrespective of case and order of appearance.Returns dataframes with column
    names with uppercase and in ascending order."""

    mismatch_occured = False
    if df1_cols != df2_cols:
        mismatch_occured = True
        mismatch_column()
        return df1, df2, mismatch_occured

    # Convert both dataframe column names to same case.
    df1 = df1.rename(columns=lambda x: x.strip())
    df2 = df2.rename(columns=lambda x: x.strip())
    df1.columns = df1.columns.str.upper()
    df2.columns = df2.columns.str.upper()

    df1_col_names = sorted(list(df1.columns.values))
    df2_col_names = sorted(list(df2.columns.values))

    if df1_col_names != df2_col_names:
        mismatch_occured = True
        mismatch_column()

    return df1, df2, mismatch_occured


def mismatch_column():
    """Print message if the columns do not match."""

    error_obj = PrettyTable()
    error_obj.field_names = ["Status", "Message"]
    error_obj.add_row(["ERROR!", "Columns in Source and Target do not match"])
    print(error_obj)


def clean_string_data(df, date_str_format):
    """Clean the dataframe for comparison."""
    df = df.applymap(str)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.replace(to_replace="9999-12-01 00:00:00.000", value="", inplace=True)
    df.replace(to_replace="9999-12-31 00:00:00.000", value="", inplace=True)
    df.replace(regex=r"\.[0]*$", value="", inplace=True)
    df.replace(
        dict.fromkeys(
            [
                "",
                "NULL",
                "nan",
                "NAN",
                "NaN",
                "None",
                "12/1/9999",
                "12/01/9999",
                "12/31/9999",
            ],
            np.NaN,
        ),
        inplace=True,
    )

    date_transform_df = transform_datetime(df, date_str_format)
    date_transform_df = date_transform_df.reset_index()

    return date_transform_df


def re_arrange_cols_and_sort(df, df_col_names):
    """Re-arrange the column and sort by values."""
    df = df.reindex(df_col_names, axis=1)
    sorted_df = df.sort_values(
        by=df_col_names,
        axis=0,
        ascending=True,
        na_position="last",
        ignore_index=True,
    )
    sorted_df.index.name = "foo"
    return sorted_df


def equalize_dataframes(left_df, right_df, df1_rows, df2_rows, df_col_names):
    """Match the dataframe rows for comparison."""

    empty_df = pd.DataFrame(
        index=range(min(df1_rows, df2_rows), max(df1_rows, df2_rows)),
        columns=df_col_names[:-1],
    )
    empty_df = empty_df.reset_index()
    empty_df.index.name = "foo"

    if df1_rows < df2_rows:
        left_df = pd.concat(
            [left_df, empty_df],
            axis=0,
        )
        left_df.index = range(len(left_df.index))
        left_df.index.name = "foo"
    elif df1_rows > df2_rows:
        right_df = pd.concat(
            [right_df, empty_df],
            axis=0,
        )
        right_df.index = range(len(right_df.index))
        right_df.index.name = "foo"

    return left_df, right_df


def separate_row_num_and_data(df, row_num_name):
    """Extract row number and the data."""

    row = df["index"]
    row = row.rename(row_num_name)
    df = df.iloc[:, :-1]
    df = df.reset_index(drop=True)
    df = df.reset_index()
    return df, row


def print_status(
    filtered_result, df1_rows, df2_rows, df_col_names, success_list, fail_list
):
    """Print result status as summary table."""

    status_obj = PrettyTable()
    status_obj.field_names = ["Status", "Source count", "Target count"]

    if (df1_rows == df2_rows) and filtered_result.empty:
        status_obj.add_row(["SUCCESS!", df1_rows, df2_rows])
        print(status_obj)
    elif (df1_rows == df2_rows) and not filtered_result.empty:
        status_obj.add_row(["FAIL!", df1_rows, df2_rows])
        print(status_obj)
    elif df1_rows != df2_rows:
        status_obj.add_row(["FAIL!", df1_rows, df2_rows])
        print(status_obj)

    summary_obj = PrettyTable()
    summary_obj.field_names = ["Summary", "Success", "Failure"]
    for (col, success, failure) in zip(df_col_names, success_list, fail_list):
        if not col == "index":
            summary_obj.add_row([col, success, failure])
    print(summary_obj)


def organize_columns(df, default_column_order):
    """Use ID columns at beginning which makes debugging easier."""
    column_distinct_value = []
    default_column_order_tmp = default_column_order[1:]
    index_col = default_column_order[:1]
    for column in default_column_order_tmp:
        column_distinct_value.append(
            [column, df.pivot_table(index=[column], aggfunc="size").size]
        )

    column_distinct_value.sort(key=lambda x: x[1], reverse=True)
    optimized_cols_order = [x[0] for x in column_distinct_value]

    return optimized_cols_order + index_col


def is_exactly_equal(left_df, right_df, df1_rows, df2_rows, df_col_names):
    """Check if the data is exactly between two dataframe."""

    l_df = left_df.drop_duplicates(
        subset=df_col_names[:-1], keep="first", inplace=False
    )
    r_df = right_df.drop_duplicates(
        subset=df_col_names[:-1], keep="first", inplace=False
    )

    l_df["_File"] = "Source"
    r_df["_File"] = "Target"

    df1_rows_distinct = len(l_df.index)
    df2_rows_distinct = len(r_df.index)

    mismatched_records = pd.concat(
        [l_df.reset_index(drop=True), r_df.reset_index(drop=True)], axis=0
    ).drop_duplicates(subset=df_col_names[:-1], keep=False, inplace=False)

    mismatched_records["index"] = mismatched_records["index"].apply(
        lambda x: int(x) + 2
    )

    if (
        df1_rows == df2_rows
        and df1_rows_distinct == df2_rows_distinct
        and mismatched_records.empty
    ):
        success_list = [df1_rows for _ in range(len(df_col_names))]
        fail_list = [0 for _ in range(len(df_col_names))]

        print_status(
            mismatched_records,
            df1_rows,
            df2_rows,
            df_col_names,
            success_list,
            fail_list,
        )
        return
    else:

        return mismatched_records


def get_difference(df1, df2, date_str_format, file_type):
    """Get row number and column name for the data difference between 2 dataframes."""
    df1_rows, df1_cols = df1.shape
    df2_rows, df2_cols = df2.shape

    df1, df2, mismatch_occured = check_column_equality(df1, df2, df1_cols, df2_cols)

    if file_type == "csv" and mismatch_occured:
        exit(0)
    elif file_type == "excel" and mismatch_occured:
        return None, None

    left_df = clean_string_data(df1, date_str_format)
    right_df = clean_string_data(df2, date_str_format)

    df_col_names = organize_columns(left_df, list(left_df.columns.values))

    left_df = re_arrange_cols_and_sort(left_df, df_col_names)
    right_df = re_arrange_cols_and_sort(right_df, df_col_names)

    mismatched_records = is_exactly_equal(
        left_df, right_df, df1_rows, df2_rows, df_col_names
    )

    if mismatched_records is not None and file_type == "csv":
        pass
    elif mismatched_records is None and file_type == "csv":
        exit(0)

    if mismatched_records is not None and file_type == "excel":
        pass
    elif mismatched_records is None and file_type == "excel":
        return None, None

    if df1_rows != df2_rows:
        left_df, right_df = equalize_dataframes(
            left_df, right_df, df1_rows, df2_rows, df_col_names
        )

    left_df, left_row = separate_row_num_and_data(left_df, "Source Row Number")
    right_df, right_row = separate_row_num_and_data(right_df, "Target Row Number")

    column_bool_list_df = get_difference_as_boolean_dataframe(
        left_df, right_df, list(left_df.columns.values)[0]
    )
    col_values = get_location_of_difference(column_bool_list_df)

    # Join Source Row, Target Row, Mismatch Column info to a dataframe.
    result = pd.concat(
        [
            left_row.reset_index(drop=True),
            right_row.reset_index(drop=True),
            col_values.reset_index(drop=True),
        ],
        axis=1,
    )

    # Since python index start from 0. Add offset 2 for readability in Excel or Text editor row number.
    result["Source Row Number"] = result["Source Row Number"].apply(
        lambda x: int(x) + 2
    )
    result["Target Row Number"] = result["Target Row Number"].apply(
        lambda x: int(x) + 2
    )

    filtered_result = result[result["Column_Name"] != ""]
    sorted_filtered_result = filtered_result.sort_values(filtered_result.columns[0])

    fail_list = column_bool_list_df.count(axis=0).to_list()
    success_list = map(lambda x: max(df1_rows, df2_rows) - x, fail_list)

    print_status(
        sorted_filtered_result,
        df1_rows,
        df2_rows,
        df_col_names,
        success_list,
        fail_list,
    )

    return sorted_filtered_result, mismatched_records


def save_xls(df_sheets_dict, xls_path):
    """Write the final dataframe to excel for all the matched sheets."""
    with pd.ExcelWriter(xls_path) as writer:
        for sheet, df in df_sheets_dict.items():
            if df is not None:
                df.to_excel(writer, sheet, index=False)
        writer.save()


def get_csv_file_path(title_name):
    """Open file explorer window and ask user to choose a CSV file."""
    root = Tk()
    root.filename = filedialog.askopenfilename(
        title=title_name, filetypes=(("CSV Files", "*.csv"),)
    )
    root.destroy()
    return root.filename


def get_excel_file_path(title_name):
    """Open file explorer window and ask user to choose a Excel file."""
    root = Tk()
    root.filename = filedialog.askopenfilename(
        title=title_name,
        filetypes=(("Excel Files", "*.xlsx"), ("Excel Files", "*.xls")),
    )
    root.destroy()
    return root.filename


def get_output_file_path_csv(title_name):
    """Open file explorer window and ask user to choose a output directory and save the output as CSV."""
    root = Tk()
    root.filename = filedialog.asksaveasfilename(
        title=title_name, filetypes=(("CSV Files", "*.csv"),)
    )
    root.destroy()
    return root.filename


def get_output_file_path_excel(title_name):
    """Open file explorer window and ask user to choose a output directory and save the output as Excel."""
    root = Tk()
    root.filename = filedialog.asksaveasfilename(
        title=title_name, filetypes=(("Excel Files", "*.xlsx"),)
    )
    root.destroy()
    return f"{root.filename}.xlsx"


def main():
    csv_or_excel = input(
        "Enter 1 to compare CSV files.\nEnter 2 to compare Excel files.\n"
    )

    try:
        if int(csv_or_excel) == 1:
            file_type = "csv"

            file_1_path = get_csv_file_path("Choose 1st file")
            file_2_path = get_csv_file_path("Choose 2nd file")

            df1 = df_from_csv(file_1_path)
            df2 = df_from_csv(file_2_path)

            date_str_format = get_format()

            final_df, extra_analysis_df = get_difference(
                df1, df2, date_str_format, file_type
            )

            try:
                if (final_df is not None) or (not final_df.empty):
                    output_path = get_output_file_path_csv(
                        "Choose output file path and name"
                    )
                    output_to_csv(final_df, output_path)

                if (extra_analysis_df is not None) or (not extra_analysis_df.empty):
                    extra_data_need = input(
                        "Enter 'yes' to save records which did not match as CSV file.\nIgnore if not needed.\n"
                    )
                    if str(extra_data_need).lower() == "yes":
                        output_path = get_output_file_path_csv(
                            "Choose output file path and name to save additional analysis data"
                        )
                        output_to_csv(extra_analysis_df, output_path)

            except ValueError:
                print("Output path not given. Result is not saved.")
            print(
                "\nTotal count of mismatching records : ",
                len(final_df.index),
                "\n",
            )

        elif int(csv_or_excel) == 2:
            file_type = "excel"

            file_1_path = get_excel_file_path("Choose 1st file")
            file_2_path = get_excel_file_path("Choose 2nd file")

            date_str_format = "%m/%d/%Y"

            left_excel = pd.ExcelFile(file_1_path)
            right_excel = pd.ExcelFile(file_2_path)

            x1_sheets = left_excel.sheet_names
            x2_sheets = right_excel.sheet_names
            common_sheets = list(set(x1_sheets).intersection(x2_sheets))

            df_sheets_dict = {}
            save_analysis_data = "no"

            for sheet in common_sheets:
                if sheet != "XDO_METADATA":
                    df1 = left_excel.parse(sheet, na_filter=False)
                    df2 = right_excel.parse(sheet, na_filter=False)

                    print(f"\nSheet: {sheet}")

                    final_df, extra_analysis_df = get_difference(
                        df1, df2, date_str_format, file_type
                    )

                    if extra_analysis_df is not None and save_analysis_data == "no":
                        extra_data_need = input(
                            "Enter 'yes' to save records which did not match as CSV file.\nIgnore if not needed.\n"
                        )
                        if str(extra_data_need).lower() == "yes":
                            save_analysis_data = "yes"
                        else:
                            save_analysis_data = "no"
                    if save_analysis_data == "yes":
                        df_sheets_dict[sheet] = final_df
                        df_sheets_dict[sheet + " analysis"] = extra_analysis_df
                    else:
                        df_sheets_dict[sheet] = final_df
            try:
                output_path = get_output_file_path_excel(
                    "Choose output file path and name"
                )
                save_xls(df_sheets_dict, output_path)
            except ValueError:
                print("Output path not given. Result is not saved.")
            except IndexError:
                print("No data to save.")
                exit(0)

        else:
            print("Not a Valid option.")
            exit(0)

    except ValueError:
        print("Not a Valid option.")
        exit(0)

    except (FileNotFoundError, IOError):
        error_obj = PrettyTable()
        error_obj.field_names = ["Status", "Message"]
        error_obj.add_row(["ERROR!", "File(s) not chosen properly"])
        print(error_obj)
        exit(0)


if __name__ == "__main__":
    main()
