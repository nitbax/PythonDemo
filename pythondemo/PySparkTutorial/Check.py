from pyspark.sql import SparkSession
from pyspark.sql.functions import length, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


def validate_schema(csv_df, schema):
    all_struct_fields = csv_df.schema
    missing_struct_fields = [x for x in schema if x not in all_struct_fields]
    error_message = (
        f"The {missing_struct_fields} StructFields are not included in the DataFrame with the following "
        f"StructFields {all_struct_fields} "
    )
    if missing_struct_fields:
        raise DataFrameMissingStructFieldError(error_message)


class DataFrameMissingStructFieldError(ValueError):
    """raise this when there's a DataFrame column error"""


def validate_schema_field_length(csv_df, schema_dict):
    df2 = (
        csv_df.withColumn(
            "length_freq",
            when(
                length(csv_df["Frequency"]) > schema_dict["Frequency"], False
            ).otherwise(True),
        )
        .withColumn(
            "length_region",
            when(length(csv_df["Region"]) > schema_dict["Region"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_curveId",
            when(length(csv_df["Curve_ID"]) > schema_dict["Curve_ID"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_Use_Case",
            when(length(csv_df["Use_Case"]) > schema_dict["Use_Case"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_group",
            when(length(csv_df["Group"]) > schema_dict["Group"], False).otherwise(True),
        )
        .withColumn(
            "length_product",
            when(length(csv_df["Product"]) > schema_dict["Product"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_currency",
            when(length(csv_df["Currency"]) > schema_dict["Currency"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_portId",
            when(length(csv_df["Port_ID"]) > schema_dict["Port_ID"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_portName",
            when(
                length(csv_df["Port_Name"]) > schema_dict["Port_Name"], False
            ).otherwise(True),
        )
        .withColumn(
            "length_URR_Rate",
            when(length(csv_df["URR_Rate"]) > schema_dict["URR_Rate"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_URR_Year",
            when(length(csv_df["URR_Year"]) > schema_dict["URR_Year"], False).otherwise(
                True
            ),
        )
        .withColumn(
            "length_Extrapolation_Method",
            when(
                length(csv_df["Extrapolation_Method"])
                > schema_dict["Extrapolation_Method"],
                False,
            ).otherwise(True),
        )
        .withColumn(
            "length_Credit_Rating",
            when(
                length(csv_df["Credit_Rating"]) > schema_dict["Credit_Rating"],
                False,
            ).otherwise(True),
        )
        .withColumn(
            "length_Liquidity",
            when(
                length(csv_df["Liquidity"]) > schema_dict["Liquidity"], False
            ).otherwise(True),
        )
        .withColumn(
            "length_Idx_Linked_Infl",
            when(
                length(csv_df["Idx_Linked_Infl"]) > schema_dict["Idx_Linked_Infl"],
                False,
            ).otherwise(True),
        )
        .withColumn(
            "length_Min_Int_Guarantee",
            when(
                length(csv_df["Min_Int_Guarantee"]) > schema_dict["Min_Int_Guarantee"],
                False,
            ).otherwise(True),
        )
        .withColumn(
            "length_Passthrough",
            when(
                length(csv_df["Passthrough"]) > schema_dict["Passthrough"], False
            ).otherwise(True),
        )
        .withColumn(
            "length_Dynamic_Lapse",
            when(
                length(csv_df["Dynamic_Lapse"]) > schema_dict["Dynamic_Lapse"],
                False,
            ).otherwise(True),
        )
    ).select(
        "length_freq",
        "length_region",
        "length_curveId",
        "length_Use_Case",
        "length_group",
        "length_product",
        "length_currency",
        "length_portId",
        "length_portName",
        "length_URR_Rate",
        "length_URR_Year",
        "length_Extrapolation_Method",
        "length_Credit_Rating",
        "length_Liquidity",
        "length_Idx_Linked_Infl",
        "length_Min_Int_Guarantee",
        "length_Passthrough",
        "length_Dynamic_Lapse",
    )
    df3 = df2.collect()
    errorMsg = ""
    for i in df3:
        freq_val = i[0]
        region_val = i[1]
        curveIdVal = i[2]
        Use_CaseVal = i[3]
        GroupVal = i[4]
        productVal = i[5]
        currencyVal = i[6]
        portIdVal = i[7]
        portNameVal = i[8]
        urrRateVal = i[9]
        urrYearVal = i[10]
        extrapolationVal = i[11]
        creditVal = i[12]
        liquidityVal = i[13]
        idxLinkedInflVal = i[14]
        minIntGuaranteeVal = i[15]
        PassthroughVal = i[16]
        Dynamic_LapseVal = i[17]
        if not freq_val:
            errorMsg += "Frequency column exceeds the permissible limits."
        if not region_val:
            errorMsg += "Region column exceeds the permissible limits."
        if not curveIdVal:
            errorMsg += "Curve ID column exceeds the permissible limits."
        if not Use_CaseVal:
            errorMsg += "Use Case column exceeds the permissible limits."
        if not GroupVal:
            errorMsg += "Group column exceeds the permissible limits."
        if not productVal:
            errorMsg += "Product column exceeds the permissible limits."
        if not currencyVal:
            errorMsg += "Currency column exceeds the permissible limits."
        if not portIdVal:
            errorMsg += "Port_ID column exceeds the permissible limits."
        if not portNameVal:
            errorMsg += "Port_Name column exceeds the permissible limits."
        if not urrRateVal:
            errorMsg += "URR_Rate column exceeds the permissible limits."
        if not urrYearVal:
            errorMsg += "URR_YEAR column exceeds the permissible limits."
        if not extrapolationVal:
            errorMsg += "Extrapolation_Method column exceeds the permissible limits."
        if not creditVal:
            errorMsg += "Credit_Rating column exceeds the permissible limits."
        if not liquidityVal:
            errorMsg += "Liquidity column exceeds the permissible limits."
        if not idxLinkedInflVal:
            errorMsg += "Idx_Linked_Infl column exceeds the permissible limits."
        if not minIntGuaranteeVal:
            errorMsg += "Min_Int_Guarantee column exceeds the permissible limits."
        if not PassthroughVal:
            errorMsg += "Passthrough column exceeds the permissible limits."
        if not Dynamic_LapseVal:
            errorMsg += "Dynamic_Lapse column exceeds the permissible limits."
        if (
            freq_val is False
            or region_val is False
            or curveIdVal is False
            or Use_CaseVal is False
            or GroupVal is False
            or productVal is False
            or currencyVal is False
            or portIdVal is False
            or portNameVal is False
            or urrRateVal is False
            or urrYearVal is False
            or extrapolationVal is False
            or creditVal is False
            or liquidityVal is False
            or idxLinkedInflVal is False
            or minIntGuaranteeVal is False
            or PassthroughVal is False
            or Dynamic_LapseVal is False
        ):
            raise DataFrameSchemaLengthMismatchError(errorMsg)


class DataFrameSchemaLengthMismatchError(ValueError):
    """raise this when there's a DataFrame column error"""


spark = SparkSession.builder.appName("SchemaCheck").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")
df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("C:/Practice SPark Work/DRC_Curve_Config.csv")
)
required_schema = StructType(
    [
        StructField("Frequency", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Curve_ID", IntegerType(), True),
        StructField("Use_Case", StringType(), True),
        StructField("Group", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("Port_ID", IntegerType(), True),
        StructField("Port_Name", StringType(), True),
        StructField("URR_Rate", DoubleType(), True),
        StructField("URR_Year", IntegerType(), True),
        StructField("Extrapolation_Method", StringType(), True),
        StructField("Credit_Rating", StringType(), True),
        StructField("Liquidity", StringType(), True),
        StructField("Idx_Linked_Infl", StringType(), True),
        StructField("Min_Int_Guarantee", StringType(), True),
        StructField("Passthrough", StringType(), True),
        StructField("Dynamic_Lapse", StringType(), True),
    ]
)
required_schema_dict = {
    "Frequency": 3,
    "Region": 6,
    "Curve_ID": 6,
    "Use_Case": 50,
    "Group": 50,
    "Product": 50,
    "Currency": 3,
    "Port_ID": 6,
    "Port_Name": 50,
    "URR_Rate": 8,
    "URR_Year": 6,
    "Extrapolation_Method": 50,
    "Credit_Rating": 5,
    "Liquidity": 5,
    "Idx_Linked_Infl": 5,
    "Min_Int_Guarantee": 5,
    "Passthrough": 5,
    "Dynamic_Lapse": 5,
}

validate_schema_field_length(df, required_schema_dict)
# validate_schema(df, required_schema)
