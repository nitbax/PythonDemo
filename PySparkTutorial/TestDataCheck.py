from datetime import datetime, date

import pyspark.sql.functions as fx
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    LongType,
    DoubleType,
)

spark = (
    SparkSession.builder.appName("Category_Summary").master("local[*]").getOrCreate()
)
spark.conf.set("spark.sql.caseSensitive", False)
RPT_MONTH = "2019-08-01"
RPT_MONTH = datetime.strptime(RPT_MONTH, "%Y-%m-%d")
COUNTY_ID = 19
HIGH_DATE = date(9999, 12, 31)
NULL_DATE = date(9999, 12, 1)


def get_category_summary(
    issuance: DataFrame,
    pgm: DataFrame,
    issuance_detl: DataFrame,
    case_vw: DataFrame,
    staff: DataFrame,
    code_detl: DataFrame,
    expngmnt: DataFrame,
    expngmnt_detl: DataFrame,
    RPT_MONTH,
    COUNTY_ID,
) -> DataFrame:
    issuance = issuance.select(
        fx.col("eff_date"),
        fx.col("ctrl_num_identif"),
        fx.col("payee_first_name"),
        fx.col("payee_mid_name"),
        fx.col("payee_last_name"),
        fx.col("payee_suffix"),
        fx.col("pmt_amt"),
        fx.col("run_date"),
        fx.col("aid_code"),
        fx.col("issue_county_code"),
        fx.col("cat_code"),
        fx.col("pgm_id"),
        fx.col("id"),
        fx.col("expngmnt_id"),
    ).alias("issuance")
    issuance_detl = issuance_detl.select(
        fx.col("stat_date"),
        fx.col("avail_date"),
        fx.col("issue_date"),
        fx.col("immed_code"),
        fx.col("type_code"),
        fx.col("stat_code"),
        fx.col("PAY_CODE"),
        fx.col("issuance_id"),
        fx.col("created_by"),
        fx.col("id"),
    ).alias("issuance_detl")
    case_vw = case_vw.select(
        fx.col("id"), fx.col("serial_num_identif"), fx.col("case_name")
    ).alias("case_vw")
    pgm = pgm.select(fx.col("id"), fx.col("case_id"), fx.col("pgm_code")).alias("pgm")
    staff = staff.select(fx.col("id"), fx.col("last_name")).alias("staff")
    code_detl = code_detl.select(
        fx.col("SHORT_DECODE_NAME"),
        fx.col("CATGRY_ID"),
        fx.col("CODE_NUM_IDENTIF"),
        fx.col("beg_date"),
        fx.col("end_date"),
    ).alias("code_detl")
    expngmnt = expngmnt.select(fx.col("id")).alias("expngmnt")
    expngmnt_detl = expngmnt_detl.select(
        fx.col("id").alias("expngmnt_detl_id"),
        fx.col("expngmnt_id"),
        fx.col("stat_code"),
    ).alias("expngmnt_detl")
    iss_iss_detl_df = issuance.join(
        issuance_detl, fx.col("issuance.id") == fx.col("issuance_detl.issuance_id")
    ).where(
        fx.to_date(fx.col("issuance_detl.stat_date")).between(
            fx.to_date(fx.lit(RPT_MONTH)), fx.to_date(fx.lit(RPT_MONTH))
        )
    )
    stat_code1_df = iss_iss_detl_df.where(
        fx.col("issuance_detl.stat_code").isin("IS", "MI")
    )

    min_id = (
        stat_code1_df.agg({"issuance_detl.id": "min"})
        .withColumnRenamed("min(id)", "min_id")
        .collect()[0][0]
    )
    warrant_type_list = (
        code_detl.where(
            (fx.col("code_detl.catgry_id") == fx.lit(313))
            & (~(fx.col("code_detl.code_num_identif") == fx.lit("RE")))
        )
        .select(
            fx.col("code_detl.code_num_identif"), fx.col("code_detl.short_decode_name")
        )
        .alias("warrant_type_list")
    )
    stat_code1_df.show(truncate=False)
    cond1 = stat_code1_df.where(
        (fx.col("issuance.issue_county_code").eqNullSafe(COUNTY_ID))
        & (~(fx.col("issuance.cat_code") == fx.lit("MB")))
        & (fx.col("issuance_detl.type_code").eqNullSafe("EB"))
        & (
            ~(fx.col("issuance_detl.PAY_CODE").isin("L1", "L2"))
            | (fx.col("issuance_detl.PAY_CODE").isNull())
        )
    )
    cond1.show(truncate=False)
    cond2 = cond1.join(pgm, fx.col("issuance.pgm_id") == fx.col("pgm.id"))
    cond3 = cond2.join(case_vw, fx.col("pgm.case_id") == fx.col("case_vw.id"))
    cond4 = cond3.join(
        warrant_type_list,
        fx.col("issuance.cat_code") == fx.col("warrant_type_list.code_num_identif"),
    )
    cond5 = cond4.join(
        staff, fx.col("issuance_detl.created_by") == fx.col("staff.id"), "left_outer"
    ).where(fx.col("issuance_detl.id").eqNullSafe(min_id))

    # Assigning values to newly created columns
    data_table = cond5.select(
        fx.date_format(fx.col("issuance.eff_date"), "MM/yyyy").alias("ben_month"),
        fx.col("warrant_type_list.short_decode_name").alias("warrant_type"),
        fx.col("issuance.ctrl_num_identif").alias("warrant_number"),
        fx.date_format(
            fx.col("issuance_detl.stat_date"), "MM/dd/yyyy hh:mm:ss a"
        ).alias("stat_date"),
        fx.col("case_vw.serial_num_identif").alias("case_number"),
        fx.col("case_vw.case_name"),
        fx.concat(
            fx.trim(
                fx.concat(
                    fx.trim(
                        fx.concat(
                            fx.col("issuance.payee_first_name"),
                            fx.lit(" "),
                            fx.substring(fx.col("issuance.payee_mid_name"), 1, 1),
                        )
                    ),
                    fx.lit(" "),
                    fx.col("issuance.payee_last_name"),
                )
            ),
            fx.lit(" "),
            fx.col("issuance.payee_suffix"),
        ).alias("payee_name"),
        fx.when(
            (fx.col("issuance_detl.avail_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.avail_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.avail_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.avail_date"), "MM/dd/yyyy"))
        .alias("auth_date"),
        fx.when(
            (fx.col("issuance_detl.issue_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.issue_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.issue_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.issue_date"), "MM/dd/yyyy"))
        .alias("print_date"),
        fx.coalesce(fx.col("issuance.pmt_amt"), fx.lit(0)).alias("print_amt"),
        fx.col("issuance_detl.immed_code"),
        fx.col("pgm.pgm_code"),
        fx.date_format(fx.col("issuance.run_date"), "MM/dd/yyyy hh:mm a").alias(
            "run_date"
        ),
        fx.col("issuance.aid_code"),
        fx.col("staff.last_name"),
    ).alias("data_table")
    data_table.show(truncate=False)
    deadline = (
        code_detl.where(
            (fx.col("CODE_DETL.CATGRY_ID") == fx.lit(2279))
            & (fx.col("CODE_DETL.CODE_NUM_IDENTIF").eqNullSafe("EC"))
        )
        .select(
            fx.to_timestamp(
                fx.concat(
                    fx.date_format(fx.lit(RPT_MONTH), "MM/dd/yyyy"),
                    fx.lit(" "),
                    fx.col("CODE_DETL.SHORT_DECODE_NAME"),
                ),
                "MM/dd/yyyy HH:mm:ss",
            ).alias("DEADLINE_DATE")
        )
        .alias("deadline")
    )

    strip = fx.udf(lambda x: x.lstrip("0"), StringType())

    category_summary1 = (
        data_table.where(fx.col("data_table.pgm_code").eqNullSafe("FS"))
        .join(
            deadline,
            (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                <= fx.col("deadline.DEADLINE_DATE")
            )
            | (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                > fx.col("deadline.DEADLINE_DATE")
            ),
        )
        .select(
            fx.lit("EBT").alias("section_identif"),
            fx.when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    <= fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host Before Daily Deadline",
            )
            .when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    > fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host After Daily Deadline",
            )
            .when(fx.col("last_name").eqNullSafe("Batch"), fx.col("last_name"))
            .otherwise(fx.lit("Manual"))
            .alias("cat_type"),
            fx.col("data_table.pgm_code"),
            fx.col("data_table.warrant_number").alias("control_num"),
            fx.col("data_table.case_number").alias("case_num"),
            fx.col("data_table.case_name"),
            fx.col("data_table.payee_name"),
            fx.col("data_table.warrant_type").alias("issuance_type"),
            fx.col("data_table.aid_code"),
            fx.col("data_table.ben_month"),
            fx.col("data_table.auth_date").alias("availiabe_date"),
            strip(fx.trim(fx.substring(fx.col("stat_date"), 1, 13))).alias(
                "process_time"
            ),
            fx.concat(fx.lit("$"), fx.col("data_table.print_amt")).alias("char_amt"),
            fx.col("data_table.print_amt").alias("amt"),
        )
        .distinct()
    )

    stat_code2_df = iss_iss_detl_df.where(
        fx.col("issuance_detl.stat_code").eqNullSafe("IS")
    )
    max_id = (
        stat_code2_df.agg({"issuance_detl.id": "max"})
        .withColumnRenamed("max(id)", "max_id")
        .collect()[0][0]
    )
    warrant_type_list1 = (
        code_detl.where(fx.col("code_detl.catgry_id") == fx.lit(313))
        .select(
            fx.col("code_detl.code_num_identif"), fx.col("code_detl.short_decode_name")
        )
        .alias("warrant_type_list1")
    )
    expngmnt_join = expngmnt_detl.join(
        expngmnt, fx.col("expngmnt_detl.expngmnt_id") == fx.col("expngmnt.id")
    )
    max_expngment_id = (
        expngmnt_join.where(fx.col("stat_code").eqNullSafe("RA"))
        .groupBy("expngmnt_detl.expngmnt_detl_id")
        .agg({"expngmnt_detl_id": "max"})
        .collect()[0][0]
    )

    category_join_cond1 = stat_code2_df.where(
        (fx.col("issuance.issue_county_code").eqNullSafe(COUNTY_ID))
        & (~(fx.col("issuance.cat_code") == fx.lit("MB")))
        & (fx.col("issuance_detl.type_code").eqNullSafe("EB"))
        & (
            ~(fx.col("issuance_detl.PAY_CODE").isin("L1", "L2"))
            | (fx.col("issuance_detl.PAY_CODE").isNull())
        )
    )
    category_join_cond2 = category_join_cond1.join(
        pgm, fx.col("issuance.pgm_id") == fx.col("pgm.id")
    )
    category_join_cond3 = category_join_cond2.join(
        case_vw, fx.col("pgm.case_id") == fx.col("case_vw.id")
    )
    category_join_cond4 = category_join_cond3.join(
        warrant_type_list1,
        fx.col("issuance.cat_code") == fx.col("warrant_type_list1.code_num_identif"),
    )
    category_join_cond5 = category_join_cond4.join(
        staff, fx.col("issuance_detl.created_by") == fx.col("staff.id"), "left_outer"
    )
    category_join_cond6 = category_join_cond5.join(
        expngmnt_join, fx.col("expngmnt.id") == fx.col("issuance.expngmnt_id")
    )
    base_data_table2 = category_join_cond6.where(
        (fx.col("expngmnt_detl.expngmnt_detl_id").eqNullSafe(max_expngment_id))
        & (fx.col("issuance_detl.id").eqNullSafe(max_id))
    )

    data_table2 = base_data_table2.select(
        fx.date_format(fx.col("issuance.eff_date"), "MM/yyyy").alias("ben_month"),
        fx.col("warrant_type_list1.short_decode_name").alias("warrant_type"),
        fx.col("issuance.ctrl_num_identif").alias("warrant_number"),
        fx.date_format(
            fx.col("issuance_detl.stat_date"), "MM/dd/yyyy hh:mm:ss a"
        ).alias("stat_date"),
        fx.col("case_vw.serial_num_identif").alias("case_number"),
        fx.col("case_vw.case_name"),
        fx.concat(
            fx.trim(
                fx.concat(
                    fx.trim(
                        fx.concat(
                            fx.col("issuance.payee_first_name"),
                            fx.lit(" "),
                            fx.substring(fx.col("issuance.payee_mid_name"), 1, 1),
                        )
                    ),
                    fx.lit(" "),
                    fx.col("issuance.payee_last_name"),
                )
            ),
            fx.lit(" "),
            fx.col("issuance.payee_suffix"),
        ).alias("payee_name"),
        fx.when(
            (fx.col("issuance_detl.avail_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.avail_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.avail_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.avail_date"), "MM/dd/yyyy"))
        .alias("auth_date"),
        fx.when(
            (fx.col("issuance_detl.issue_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.issue_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.issue_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.issue_date"), "MM/dd/yyyy"))
        .alias("print_date"),
        fx.col("issuance.pmt_amt").alias("auth_amt"),
        fx.col("issuance.pmt_amt").alias("print_amt"),
        fx.col("issuance_detl.immed_code"),
        fx.col("pgm.pgm_code"),
        fx.date_format(fx.col("issuance.run_date"), "MM/dd/yyyy HH:mm").alias(
            "run_date"
        ),
        fx.col("issuance.aid_code"),
        fx.col("staff.last_name"),
    ).alias("data_table2")

    category_summary2 = (
        data_table2.where(fx.col("data_table2.pgm_code").eqNullSafe("FS"))
        .join(
            deadline,
            (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                <= fx.col("deadline.DEADLINE_DATE")
            )
            | (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                > fx.col("deadline.DEADLINE_DATE")
            ),
        )
        .select(
            fx.lit("Reactivation").alias("section_identif"),
            fx.when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    <= fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host Before Daily Deadline",
            )
            .when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    > fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host After Daily Deadline",
            )
            .when(fx.col("last_name").eqNullSafe("Batch"), fx.col("last_name"))
            .otherwise(fx.lit("Manual"))
            .alias("cat_type"),
            fx.col("data_table2.pgm_code"),
            fx.col("data_table2.warrant_number").alias("control_num"),
            fx.col("data_table2.case_number").alias("case_num"),
            fx.col("data_table2.case_name"),
            fx.col("data_table2.payee_name"),
            fx.col("data_table2.warrant_type").alias("issuance_type"),
            fx.col("data_table2.aid_code"),
            fx.col("data_table2.ben_month"),
            fx.col("data_table2.auth_date").alias("availiabe_date"),
            strip(fx.trim(fx.substring(fx.col("stat_date"), 1, 13))).alias(
                "process_time"
            ),
            fx.concat(fx.lit("$"), fx.col("data_table2.print_amt")).alias("char_amt"),
            fx.col("data_table2.print_amt").alias("amt"),
        )
        .distinct()
    )

    category_summary12 = category_summary1.union(category_summary2)

    stat_code3_df = iss_iss_detl_df.where(
        fx.col("issuance_detl.stat_code").eqNullSafe("CA")
    )

    max_id3 = (
        stat_code3_df.agg({"issuance_detl.id": "max"})
        .withColumnRenamed("max(id)", "max_id")
        .collect()[0][0]
    )
    aid_code_df = (
        code_detl.where(
            (fx.col("catgry_id") == fx.lit(184))
            & (
                fx.to_timestamp(fx.lit(RPT_MONTH)).between(
                    fx.to_timestamp(fx.col("beg_date")),
                    fx.to_timestamp(fx.col("end_date")),
                )
            )
        )
        .select(fx.col("short_decode_name"), fx.col("code_num_identif"))
        .alias("aid_code_df")
    )

    category_join_final1 = stat_code3_df.where(
        (fx.col("issuance.issue_county_code").eqNullSafe(COUNTY_ID))
        & (fx.col("issuance_detl.type_code").eqNullSafe("EB"))
        & (
            ~(fx.col("issuance_detl.PAY_CODE").isin("L1", "L2"))
            | (fx.col("issuance_detl.PAY_CODE").isNull())
        )
    )
    category_join_final2 = category_join_final1.join(
        pgm, fx.col("issuance.pgm_id") == fx.col("pgm.id")
    )
    category_join_final3 = category_join_final2.join(
        case_vw, fx.col("pgm.case_id") == fx.col("case_vw.id")
    )
    category_join_final4 = category_join_final3.join(
        warrant_type_list1,
        fx.col("issuance.cat_code") == fx.col("warrant_type_list1.code_num_identif"),
    )
    category_join_final5 = category_join_final4.join(
        staff, fx.col("issuance_detl.created_by") == fx.col("staff.id"), "left_outer"
    )
    category_join_final6 = category_join_final5.join(
        expngmnt_join, fx.col("expngmnt.id") == fx.col("issuance.expngmnt_id")
    )
    base_data_table3 = category_join_final6.join(
        aid_code_df,
        fx.col("issuance.aid_code") == fx.col("aid_code_df.code_num_identif"),
    ).where(fx.col("issuance_detl.id").eqNullSafe(max_id3))

    data_table3 = base_data_table3.select(
        fx.date_format(fx.col("issuance.eff_date"), "MM/yyyy").alias("ben_month"),
        fx.col("warrant_type_list1.short_decode_name").alias("warrant_type"),
        fx.col("issuance.ctrl_num_identif").alias("warrant_number"),
        fx.date_format(
            fx.col("issuance_detl.stat_date"), "MM/dd/yyyy hh:mm:ss a"
        ).alias("stat_date"),
        fx.date_format(fx.col("issuance_detl.stat_date"), "MM/dd/yyyy").alias(
            "cancelled_date"
        ),
        fx.col("case_vw.serial_num_identif").alias("case_number"),
        fx.col("case_vw.case_name"),
        fx.concat(
            fx.trim(
                fx.concat(
                    fx.trim(
                        fx.concat(
                            fx.col("issuance.payee_first_name"),
                            fx.lit(" "),
                            fx.substring(fx.col("issuance.payee_mid_name"), 1, 1),
                        )
                    ),
                    fx.lit(" "),
                    fx.col("issuance.payee_last_name"),
                )
            ),
            fx.lit(" "),
            fx.col("issuance.payee_suffix"),
        ).alias("payee_name"),
        fx.when(
            (fx.col("issuance_detl.avail_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.avail_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.avail_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.avail_date"), "MM/dd/yyyy"))
        .alias("auth_date"),
        fx.when(
            (fx.col("issuance_detl.issue_date") == fx.lit(HIGH_DATE))
            | (fx.col("issuance_detl.issue_date") == fx.lit(NULL_DATE))
            | (fx.col("issuance_detl.issue_date").isNull()),
            " ",
        )
        .otherwise(fx.date_format(fx.col("issuance_detl.issue_date"), "MM/dd/yyyy"))
        .alias("print_date"),
        (fx.coalesce(fx.col("issuance.pmt_amt"), fx.lit(0)) * -1).alias("print_amt"),
        fx.col("issuance_detl.immed_code"),
        fx.col("pgm.pgm_code"),
        fx.col("staff.last_name"),
        fx.col("aid_code_df.short_decode_name").alias("aid_code"),
    ).alias("data_table3")

    category_summary3 = (
        data_table3.where(fx.col("data_table3.pgm_code").eqNullSafe("FS"))
        .join(
            deadline,
            (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                <= fx.col("deadline.DEADLINE_DATE")
            )
            | (
                fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss")
                > fx.col("deadline.DEADLINE_DATE")
            ),
        )
        .select(
            fx.lit("Cancelled").alias("section_identif"),
            fx.when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    <= fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host Before Daily Deadline",
            )
            .when(
                (fx.col("last_name").eqNullSafe("Host-to-Host"))
                & (
                    fx.to_timestamp(fx.col("STAT_DATE"), "MM/dd/yyyy hh:mm:ss a")
                    > fx.col("deadline.DEADLINE_DATE")
                ),
                "Host to Host After Daily Deadline",
            )
            .when(fx.col("last_name").eqNullSafe("Batch"), fx.col("last_name"))
            .otherwise(fx.lit("Manual"))
            .alias("cat_type"),
            fx.col("data_table3.pgm_code"),
            fx.col("data_table3.warrant_number").alias("control_num"),
            fx.col("data_table3.case_number").alias("case_num"),
            fx.col("data_table3.case_name"),
            fx.col("data_table3.payee_name"),
            fx.col("data_table3.warrant_type").alias("issuance_type"),
            fx.col("data_table3.aid_code"),
            fx.col("data_table3.ben_month"),
            fx.col("data_table3.auth_date").alias("availiabe_date"),
            strip(fx.trim(fx.substring(fx.col("stat_date"), 1, 13))).alias(
                "process_time"
            ),
            fx.concat(fx.lit("$"), fx.col("data_table3.print_amt")).alias("char_amt"),
            fx.col("data_table3.print_amt").alias("amt"),
        )
        .distinct()
    )

    details = category_summary12.union(category_summary3).alias("details")
    issuance_count_cs_df = details.groupBy(fx.col("amt")).count()
    all_details = details.join(issuance_count_cs_df, ["amt"])
    window_spec1 = Window.rangeBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    category_summary_final1 = all_details.select(
        fx.col("cat_type").alias("cat_type_cs"),
        fx.col("issuance_type").alias("issuance_type_cs"),
        fx.col("availiabe_date").alias("available_date_cs"),
        fx.col("count").alias("issuance_count_cs"),
        fx.sum(fx.col("amt")).over(window_spec1).alias("sub_total_amt_cs"),
    )
    l1 = []
    schema = StructType(
        [
            StructField("cat_type_cs", StringType(), True),
            StructField("issuance_type_cs", StringType(), True),
            StructField("available_date_cs", StringType(), True),
            StructField("issuance_count_cs", LongType(), True),
            StructField("sub_total_amt_cs", DoubleType(), True),
        ]
    )
    empty_df = spark.createDataFrame(l1, schema)
    category_summary_result = category_summary_final1.union(empty_df).orderBy(
        fx.col("cat_type_cs"), fx.col("issuance_type_cs"), fx.col("available_date_cs")
    )

    return category_summary_result


def load_all_data():
    pgm = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/pgm.csv"
    )
    issuance = spark.read.option("header", True).csv("C:/Test Data/Report/issuance.csv")
    issuance_detl = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/issuance_detl.csv"
    )
    case_vw = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/case_vw.csv"
    )
    staff = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/staff.csv"
    )
    code_detl = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/code_detl.csv"
    )
    expngmnt = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/expngmnt.csv"
    )
    expngmnt_detl = spark.read.option("header", True).csv(
        "C:/Test Data/Category Summary_csv data files/expngmnt_detl.csv"
    )
    df = get_category_summary(
        issuance=issuance,
        pgm=pgm,
        issuance_detl=issuance_detl,
        case_vw=case_vw,
        staff=staff,
        code_detl=code_detl,
        expngmnt=expngmnt,
        expngmnt_detl=expngmnt_detl,
        RPT_MONTH=RPT_MONTH,
        COUNTY_ID=COUNTY_ID,
    )
    df.show(truncate=False)


if __name__ == "__main__":
    load_all_data()
