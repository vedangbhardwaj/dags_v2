import snowflake.connector
import boto3
import statsmodels.api as sm
import logging
from airflow.models import Variable
from uw_utility_functions_v2 import write_to_snowflake, get_connector
import os
from datetime import datetime, timedelta

model_path = "underwriting_assets/combination_model_xg/models_v2/"

config = Variable.get("underwriting_dags_v2", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]

config_var = Variable.get("underwriting_dags_v2", deserialize_json=True)[
    "combination_xgb_weights"
]
config_thresh = Variable.get("underwriting_dags_v2", deserialize_json=True)[
    "xgboost_thresholds"
]

conn = get_connector()
cur = conn.cursor()


def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.{identifier}_{dataset_name}_dag_version_two"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return

def predict(dataset_name, **kwargs):
    import pickle
    import numpy as np
    import pandas as pd
    from uw_sql_queries_v2 import Get_query

    start_date = kwargs["ti"].xcom_pull(key="start_date")
    # start_date = "2022-08-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    # end_date = "2023-02-21"
    print(f"*********************** start_date: {start_date}***********************")
    print(f"*********************** end_date: {end_date} ***********************")

    def get_raw_data(start_date, end_date):
        sql_cmd = Get_query("COMBINATION_MODEL_XG").get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    def get_data(module_name):
        sql_cmd = None
        if module_name == "KB_TXN_MODULE":
            sql_cmd = Get_query("COMBINATION_MODEL_XG").get_txn_data
        if module_name == "KB_SMS_MODULE":
            sql_cmd = Get_query("COMBINATION_MODEL_XG").get_sms_data
        if module_name == "KB_BUREAU_MODULE":
            sql_cmd = Get_query("COMBINATION_MODEL_XG").get_bureau_data
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    BRE_data = get_raw_data(start_date, end_date)
    Transaction_module_data = get_data("KB_TXN_MODULE")
    Bureau_module_data = get_data("KB_BUREAU_MODULE")
    Sms_module_data = get_data("KB_SMS_MODULE")

    Transaction_module_data.rename(
        columns={"PRED_TRAIN_XGB": "Trx_PD", "LOGODDS_SCORE": "logodds_trx"},
        inplace=True,
    )
    Sms_module_data.rename(
        columns={"PRED_TRAIN_XGB": "SMS_PD", "LOGODDS_SCORE": "logodds_sms"},
        inplace=True,
    )
    Bureau_module_data.rename(
        columns={"PRED_TRAIN_XGB": "Br_PD", "LOGODDS_SCORE": "logodds_br"}, inplace=True
    )
    BRE_data.rename(
        columns={"BRE_RUN_DATE": "DISBURSED_DATE", "CUSTOMER_ID": "USER_ID"},
        inplace=True,
    )

    # Sms_module_data["NUM_READABLE_SMS_180D"] = pd.to_numeric(Sms_module_data["NUM_READABLE_SMS_180D"])
    BRE_data["NUM_READABLE_SMS_180D"] = pd.to_numeric(BRE_data["NUM_READABLE_SMS_180D"])
    BRE_data["BUREAUSCORE"] = pd.to_numeric(BRE_data["BUREAUSCORE"])
    BRE_data["TOTAL_TRXNS"] = pd.to_numeric(BRE_data["TOTAL_TRXNS"])
    BRE_data["DAYS_SINCE_LAST_TXN"] = pd.to_numeric(BRE_data["DAYS_SINCE_LAST_TXN"])

    BRE_data["IS_SMS_FEATURE"] = [
        True if x > 0 else False for x in BRE_data["NUM_READABLE_SMS_180D"]
    ]
    Sms_module_data["IS_SMS_FEATURE"] = [
        True if x > 0 else False for x in Sms_module_data["NUM_READABLE_SMS_180D"]
    ]
    Sms_module_data = Sms_module_data.loc[(Sms_module_data["IS_SMS_FEATURE"] == True)]
    # BRE_data = BRE_data.loc[(BRE_data["IS_SMS_FEATURE"] == True)]

    def combination_type(data):
        if data["IS_SMS_SUFF"] == "NO":
            return "SMS_INSUFFICIENT"
        elif (
            (data["IS_SMS_SUFF"] == "YES")
            & (data["IS_BR_SUFF"] == "YES")
            & (data["IS_TRX_SUFF"] == "YES")
        ):
            return "SMS_BR_TRX_SUFFICIENT"
        elif (
            (data["IS_SMS_SUFF"] == "YES")
            & (data["IS_BR_SUFF"] == "NO")
            & (data["IS_TRX_SUFF"] == "YES")
        ):
            return "SMS_TRX_SUFFICIENT"
        elif (
            (data["IS_SMS_SUFF"] == "YES")
            & (data["IS_BR_SUFF"] == "YES")
            & (data["IS_TRX_SUFF"] == "NO")
        ):
            return "SMS_BR_SUFFICIENT"
        elif (
            (data["IS_SMS_SUFF"] == "YES")
            & (data["IS_BR_SUFF"] == "NO")
            & (data["IS_TRX_SUFF"] == "NO")
        ):
            return "SMS_SUFFICIENT"
        else:
            return "INSUFFICIENT"

    BRE_data["IS_BR_SUFF"] = [
        "YES" if x >= 300 else "NO" for x in BRE_data["BUREAUSCORE"]
    ]
    # Sms_module_data["IS_SMS_SUFF"] = [
    #     "YES" if x > 0 else "NO" for x in Sms_module_data["NUM_READABLE_SMS_180D"]
    # ]
    BRE_data["IS_SMS_SUFF"] = [
        "YES" if x > 0 else "NO" for x in BRE_data["NUM_READABLE_SMS_180D"]
    ]
    BRE_data["IS_TRX_SUFF"] = [
        "YES" if ((x <= 90) & (pd.isna(x) == False) & (y >= 10)) else "NO"
        for (x, y) in zip(BRE_data["DAYS_SINCE_LAST_TXN"], BRE_data["TOTAL_TRXNS"])
    ]

    BRE_data["COMBINATION_TYPE"] = BRE_data.apply(combination_type, axis=1)

    BRE_data["DISBURSED_DATE"] = pd.to_datetime(
        BRE_data[f"DISBURSED_DATE"]
    ).dt.strftime("%Y-%m-%d")
    Sms_module_data["DISBURSED_DATE"] = pd.to_datetime(
        Sms_module_data[f"DISBURSED_DATE"]
    ).dt.strftime("%Y-%m-%d")

    data_merge = BRE_data.merge(
        Bureau_module_data[
            [
                "BRE_RUN_ID",
                "Br_PD",
                "logodds_br",
            ]
        ],
        on=["BRE_RUN_ID"],
        how="left",
    )
    data_merge = data_merge.drop_duplicates()

    data_merge = data_merge.merge(
        Transaction_module_data[
            [
                # ["USER_ID", "LOAN_ID", "DISBURSED_DATE", "BAD_FLAG", "act_logodds"]
                "BRE_RUN_ID",
                "Trx_PD",
                "logodds_trx",
            ]
        ],
        on=["BRE_RUN_ID"],
        how="left",
    )
    data_merge = data_merge.drop_duplicates()

    data_merge = data_merge.merge(
        Sms_module_data[
            [
                "BRE_RUN_ID",
                "SMS_PD",
                "logodds_sms",
            ]
        ],
        on=["BRE_RUN_ID"],
        how="left",
    )
    data_merge = data_merge.drop_duplicates()

    def combined_score(data):
        if (
            data["COMBINATION_TYPE"].lower().strip()
            == "SMS_TRX_SUFFICIENT".lower().strip()
        ):
            return 1 / (
                1
                + np.exp(
                    -(
                        (50 / 100) * data["logodds_trx"]
                        + (50 / 100) * data["logodds_sms"]
                    )
                )
            )
        elif (
            data["COMBINATION_TYPE"].lower().strip()
            == "SMS_BR_SUFFICIENT".lower().strip()
        ):
            return 1 / (
                1
                + np.exp(
                    -(
                        (40 / 100) * data["logodds_br"]
                        + (60 / 100) * data["logodds_sms"]
                    )
                )
            )
        elif (
            data["COMBINATION_TYPE"].lower().strip()
            == "SMS_BR_TRX_SUFFICIENT".lower().strip()
        ):
            return 1 / (
                1
                + np.exp(
                    -(
                        (33 / 100) * data["logodds_trx"]
                        + (34 / 100) * data["logodds_sms"]
                        + (33 / 100) * data["logodds_br"]
                    )
                )
            )
        else:
            return None

    data_merge["COMBINED_PD"] = data_merge.apply(combined_score, axis=1)

    data_merge["COMBINED_LOGODDS"] = np.log(
        data_merge["COMBINED_PD"] / (1 - data_merge["COMBINED_PD"])
    )
    print(f"************************************* PANDAS VERSION {pd.__version__}*************************************")
    Final_calib = pickle.loads(
        s3.Bucket(s3_bucket)
        .Object(f"{model_path}Model_Final_calibration.pkl")
        .get()["Body"]
        .read()
    )
    data_merge["CALIB_PD"] = Final_calib.predict(
        sm.add_constant(data_merge["COMBINED_LOGODDS"])
    )
    data_merge = data_merge.drop_duplicates()

    print(f" combination_train final ***********{data_merge.shape}***********")
    # print(data_merge.columns)

    def Risk_bucket(data):
        if data["CALIB_PD"] <= 0.0162:
            return "A"
        elif (data["CALIB_PD"] > 0.0162) & (data["CALIB_PD"] <= 0.0267):
            return "B"
        elif (data["CALIB_PD"] > 0.0267) & (data["CALIB_PD"] <= 0.0392):
            return "C"
        elif (data["CALIB_PD"] > 0.0392) & (data["CALIB_PD"] <= 0.053163138):
            return "D"
        elif data["CALIB_PD"] > 0.053163138:
            return "E"
        else:
            return "Missing"    

    data_merge["Risk_bands"] = data_merge.apply(Risk_bucket, axis=1)

    data_merge.rename(
        columns={"DISBURSED_DATE": "BRE_RUN_DATE", "USER_ID": "CUSTOMER_ID"},
        inplace=True,
    )
    # data_merge["BRE_RUN_DATE_TIME"] = pd.to_datetime(data_merge[f"BRE_RUN_DATE_TIME"])
    # data_merge.drop(["BRE_RUN_DATE_TIME"], axis=1, inplace=True)

    truncate_table("final_result_rule_add", "COMBINATION_MODEL_XG".lower())
    write_to_snowflake(data_merge, "final_result_rule_add", "COMBINATION_MODEL_XG".lower())
    return

def result_generation(dataset_name,**kwargs):
    from uw_sql_queries_v2 import Get_query
    from uw_policy_rules_dag_v2 import apply_rules, policy_with_fb
    import pandas as pd

    # start_date = kwargs["ti"].xcom_pull(key="start_date")
    # start_date = "2023-02-21"
    # end_date = datetime.now().strftime("%Y-%m-%d")
    # end_date = "2023-02-23"

    def get_data():
        sql_query = Get_query("COMBINATION_MODEL_XG").get_policy_run_gen_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    def rule_engine_verdict(data):
        data["MODEL_TYPE"] = "XGBOOST"
        score_columns = [
            # ("SMS_PD", config_thresh["SMS_PD"]),
            # ("Trx_PD", config_thresh["TRX_PD"]),
            # ("Br_PD", config_thresh["BR_PD"]),
            # ("CALIB_PD", config_thresh["CALIB_PD"]),
        ]

        data["BUREAUSCORE"] = pd.to_numeric(data["BUREAUSCORE"])
        data["NTC_flag"] = [
            1 if (x < 300 or pd.isna(x)) else 0 for x in data["BUREAUSCORE"]
        ]
        # data['Rule1:MCA']=['PASS' if x>=5000 else 'FAIL' if x<5000 else 'NOT_DECIDED' for x in data['TOTAL_CREDIT_FOR_RULE']]
        data["Rule2_OVERALL_BOUNCE_M0123_rule_decision"] = [
            "PASS" if (x < 5) else "FAIL" if (x >= 5) else "NOT_DECIDED"
            for x in data["OVERALL_BOUNCE_M0123"]
        ]
        # data['Rule3:device_connect_rule']=['PASS' if (x<5) else 'FAIL' if (x>=5) else 'NOT_DECIDED'  for x in data['OVERALL_BOUNCE_M0123']]
        data["Rule4_AUTO_DEBIT_BOUNCE_rule_decision"] = [
            "PASS"
            if (x == "false" or y == "false")
            else "FAIL"
            if (x == "true" and y == "true")
            else "NOT_DECIDED"
            for (x, y) in zip(
                data["AUTO_DEBIT_BOUNCE_M0"], data["AUTO_DEBIT_BOUNCE_M1"]
            )
        ]
        data["Rule5_WRITTEN_OFF_SETTLED_rule_decision"] = [
            "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
            for x in data["WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE"]
        ]
        data["Rule6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_decision"] = [
            "PASS" if x <= 500 else "FAIL" if x > 500 else "NOT_DECIDED"
            for x in data["WRITTEN_OFF_AMT_TOTAL_FOR_RULE"]
        ]
        data["Rule7_No_foll_suit_filed_default_RULE_decision"] = [
            "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
            for x in data["SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE"]
        ]
        data["Rule8_No_foll_suit_filed_Written_OFF_RULE_decision"] = [
            "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
            for x in data["SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE"]
        ]
        data["Rule9_MAX_DPD_2_YEARS_FOR_RULE"] = [
            "PASS" if x <= 30 else "FAIL" if x > 30 else "NOT_DECIDED"
            for x in data["MAX_DPD_2_YEARS_FOR_RULE"]
        ]
        data["Rule10_No_foll_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"] = [
            "PASS" if x == 0 else "FAIL" if x > 0 else "NOT_DECIDED"
            for x in data["PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE"]
        ]
        data["Rule11_No_foll_status_in_24M_rule"] = [
            "PASS" if (x == 0) else "FAIL" if x > 0 else "NOT_DECIDED"
            for x in data["NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE"]
        ]
        data["Rule12_Experian_score"] = [
            "PASS"
            if x >= 700
            else "FAIL"
            if (x < 700 and x >= 300)
            else "NOT_DECIDED"
            if (x < 300 or pd.isna(x))
            else "NOT_DECIDED"
            for x in data["BUREAUSCORE"]
        ]
        data["Rule13_NTC_FIS_PD_score_rule_decision"] = [
            "PASS"
            if (x == 1 and y <= 0.0284)
            else "FAIL"
            if (x == 1 and y > 0.0284)
            else "NOT_DECIDED"
            for (x, y) in zip(
                data["NTC_flag"], data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
            )
        ]
        data["Rule14_FIS_PD_score_rule_decision"] = [
            "PASS" if (x <= 0.0354) else "FAIL" if (x > 0.0354) else "NOT_DECIDED"
            for x in data["SCORE_CD_V2_EMSEMBLE_PROBABILITY"]
        ]
        data["Rule15:NUM_LOAN_DEFAULTS_30D"] = [
            "FAIL"
            if ((x + y > 3) or z > 0 or w > 0)
            else "PASS"
            if ((x + y <= 3) and z == 0 and w == 0)
            else "NOT_DECIDED"
            for (x, y, z, w) in zip(
                data["NUM_LOAN_OVERDUE_30D"],
                data["NUM_CREDIT_CARD_OVERDUE_30D"],
                data["NUM_LOAN_DEFAULT_30D"],
                data["NUM_CREDIT_CARD_DEFAULT_30D"],
            )
        ]
        
        # removing col NUM_CREDIT_CARD_DEFAULT_30D
        # data["Rule15:NUM_LOAN_DEFAULTS_30D"] = [
        #     "FAIL"
        #     if ((x + y > 3) or z > 0 )
        #     else "PASS"
        #     if ((x + y <= 3) and z == 0 )
        #     else "NOT_DECIDED"
        #     for (x, y, z) in zip(
        #         data["NUM_LOAN_OVERDUE_30D"],
        #         data["NUM_CREDIT_CARD_OVERDUE_30D"],
        #         data["NUM_LOAN_DEFAULT_30D"]
        #     )
        # ]

        data[f"verdict_policy_alone"] = data.apply(apply_rules, axis=1)
        data[f"verdict_policy_with_fb"] = [
            "FAIL" if (x == "FAIL" or y == "FAIL" or z == "FAIL") else "PASS"
            for (x, y, z) in zip(
                data["verdict_policy_alone"],
                data["Rule14_FIS_PD_score_rule_decision"],
                data["Rule13_NTC_FIS_PD_score_rule_decision"],
            )
        ]
        # for val in score_columns:
        #     col = val[0]
        #     thresholds = val[1]
        #     count = 1
        #     for threshold in thresholds:
        #         data[f"{col}_verdict_model_thresh_{count}"] = data[f"{col}"].apply(
        #             lambda x: "NOT_DECIDED"
        #             if pd.isna(x)
        #             else "FAIL"
        #             if ~pd.isna(x) and x > threshold
        #             else "PASS"
        #         )
        #         data[f"{col}_verdict_policy_and_model_thresh_{count}"] = data.apply(
        #             # lambda x: apply_rules(x, x[f"{col}"], threshold, 'model_and_policy'), axis=1
        #             lambda x: "FAIL"
        #             if x["verdict_policy_alone"] == "FAIL"
        #             or x[f"{col}_verdict_model_thresh_{count}"] == "FAIL"
        #             else "NOT_DECIDED"
        #             if x[f"{col}_verdict_model_thresh_{count}"] == "NOT_DECIDED"
        #             else "PASS",
        #             axis=1,
        #         )
        #         count += 1
        return data

    data = get_data()
    verdict_data = rule_engine_verdict(data)
    truncate_table("verdict_result_rule_add", "COMBINATION_MODEL_XG".lower())
    write_to_snowflake(verdict_data, "verdict_result_rule_add", "COMBINATION_MODEL_XG".lower())
    return

def merge_master_table(dataset_name):
    import pandas as pd
    from uw_sql_queries_v2 import Get_query

    def merge_data():
        sql_query = Get_query("COMBINATION_MODEL_XG").merge_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")

    merge_data()
    return

