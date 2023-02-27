import copy
import logging
import os
from itertools import product
import boto3
import pandas as pd
import snowflake.connector
from airflow.models import Variable
from sklearn.metrics import auc, roc_curve
from uw_sql_queries_v2 import Get_query
from datetime import datetime, timedelta
from uw_utility_functions_v2 import (
    write_to_snowflake,
    get_connector,
    ks,
    calculate_psi_cat,
    calculate_psi_num,
)

config = Variable.get("underwriting_dags_v2", deserialize_json=True)
s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]

conn = get_connector()
cur = conn.cursor()


def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.{identifier}_{dataset_name}_dag_version_two"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return


def merge_repayments_data(dataset_name):
    from uw_sql_queries_v2 import Get_query

    start_date = "2022-08-01"
    end_date = "2022-10-31"
    # end_date = datetime.now().strftime("%Y-%m-%d")

    def merge_data(start_date, end_date):
        sql_query = Get_query("MASTER_TABLE_GENERATION").merge_master_table.format(
            sd=start_date, ed=end_date
        )
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    data = merge_data(start_date, end_date)
    print(f"{data.shape}")
    # data = data.drop_duplicates()
    # print(f"post dropping duplicates {data.shape}")
    return


merge_repayments_data("MASTER_TABLE_GENERATION")


def calculate_gini_score(dataset_name):
    def get_data(dataset_name):
        sql_query = Get_query(dataset_name).get_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    data = get_data(dataset_name)
    data.shape
    data["MONTH_YEAR"] = pd.to_datetime(data["DISBURSED_DATE"]).dt.strftime("%b-%Y")
    model_types = list(data["MODEL_TYPE"].value_counts().keys().values)
    months = list(data["MONTH_YEAR"].value_counts().keys().values)
    comb_model_month = list(product(months, model_types))
    gini_cols = [
        ["CALIB_PD", True],
        ["SMS_PD", "IS_SMS_FEATURES_AVAILABLE"],
        ["TRX_PD", "IS_LEDGER_FEATURES_AVAILABLE"],
        ["BR_PD", "IS_BUREAU_FEATURES_AVAILBALE"],
        ["SCORE_CD_V2_EMSEMBLE_PROBABILITY", True],
    ]

    for columns in gini_cols:
        col = columns[0]
        col_check = columns[1]
        for model in model_types:
            data1 = copy.deepcopy(data)
            data1 = data1.loc[
                pd.to_datetime(data1[f"DISBURSED_DATE"]).dt.strftime("%Y-%m-%d")
                >= "2022-08-01",
                :,
            ]
            data1 = data1.groupby(["MODEL_TYPE"])
            data1 = data1.get_group((f"{model}"))
            if col == "CALIB_PD" or col == "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                data1 = data1[(data1[f"{col}"].notnull())]
            else:
                data1 = data1[
                    (data1[f"{col}"].notnull()) & (data1[f"{col_check}"] == True)
                ]

            fpr, tpr, thresholds = roc_curve(
                data1["EVER_15DPD_IN_90DAYS"], data1[f"{col}"]
            )
            roc_auc = auc(fpr, tpr)
            GINI_score = (2 * roc_auc) - 1
            data1[f"{col}_GINI_OVERALL"] = GINI_score
            data.loc[data.index.isin(data1.index), [f"{col}_GINI_OVERALL"]] = data1[
                [f"{col}_GINI_OVERALL"]
            ].values
        for combinations in comb_model_month:
            month = combinations[0]
            model = combinations[1]
            df = data.groupby(["MONTH_YEAR", "MODEL_TYPE"])
            df = df.get_group((f"{month}", f"{model}"))
            if col == "CALIB_PD" or col == "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                df1 = df[df[f"{col}"].notnull()]
            else:
                df1 = df[df[f"{col}"].notnull() & (df[f"{col_check}"] == True)]
            # print(f"Subset dataframe shape **********************{df.shape}**********************")
            fpr, tpr, thresholds = roc_curve(df1["EVER_15DPD_IN_90DAYS"], df1[f"{col}"])
            roc_auc = auc(fpr, tpr)
            GINI_score = (2 * roc_auc) - 1
            df1[f"{col}_GINI"] = GINI_score
            # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
            data.loc[data.index.isin(df1.index), [f"{col}_GINI"]] = df1[
                [f"{col}_GINI"]
            ].values
            if col != "SCORE_CD_V2_EMSEMBLE_PROBABILITY":
                if col == "BR_PD":
                    df2 = df[
                        (df[f"{col}"].notnull())
                        & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
                        & (df[f"{col_check}"] == True)
                    ]
                else:
                    df2 = df[
                        (df[f"{col}"].notnull())
                        & (df["SCORE_CD_V2_EMSEMBLE_PROBABILITY"].notnull())
                        # & (df[f"{col_check}"]==True)
                    ]
                fpr, tpr, thresholds = roc_curve(
                    df2["EVER_15DPD_IN_90DAYS"],
                    df2[f"SCORE_CD_V2_EMSEMBLE_PROBABILITY"],
                )
                roc_auc = auc(fpr, tpr)
                GINI_score = (2 * roc_auc) - 1
                df2[f"{col}_AND_FB_SCORE_GINI"] = GINI_score
                # print(f"GINI score for {model} and {month} and {col} dataframe shape **********************{GINI_score}**********************")
                data.loc[
                    data.index.isin(df2.index), [f"{col}_AND_FB_SCORE_GINI"]
                ] = df2[[f"{col}_AND_FB_SCORE_GINI"]].values

    truncate_table("verdict_with_gini_psi_rule_add", dataset_name.lower())
    write_to_snowflake(data, "verdict_with_gini_psi_rule_add", dataset_name.lower())


calculate_gini_score("MASTER_TABLE_GENERATION")


def calculate_ks_score(dataset_name):
    def get_data(dataset_name):
        sql_query = Get_query(dataset_name).get_master_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    data = get_data(dataset_name)
    ks_score_cols = [
        "CALIB_PD",
        "SMS_PD",
        "TRX_PD",
        "BR_PD",
    ]
    model_types = list(data["MODEL_TYPE"].value_counts().keys().values)
    df_result = pd.DataFrame(
        columns=[
            "PRED_COL",
            "Min_prob",
            "Max_prob",
            "Bads",
            "Goods",
            "Distribution Goods",
            "Distribution Bads",
            "%Cum_bads",
            "%Cum_goods",
            "%Cum_difference",
        ]
    )

    for pred_col in ks_score_cols:
        for model in model_types:
            data_subset = ks(
                data=data, target="EVER_15DPD_IN_90DAYS", prob=pred_col, model=model
            )
            prediction_col_name = pred_col + "_" + model
            data_subset["PRED_COL"] = prediction_col_name
            df_result = df_result.append(data_subset)

    truncate_table(df_result, "modules_ks_scores")
    write_to_snowflake(df_result, "modules_ks_scores", dataset_name.lower())


calculate_ks_score("MASTER_TABLE_GENERATION")


def calculate_PSI(dataset_name):
    def var_type(var1):
        if var1 in cat_col:
            return "Categorical"
        elif var1 in num_col:
            return "Numerical"
        else:
            return "Others"

    def missing_ind_convert_num(df):
        for var in df.columns:
            if var_type(var) == "Numerical":
                df[var] = pd.to_numeric(df[var])
                df[var] = df[var].fillna(missing_value_num)
        for var in df.columns:
            if var_type(var) == "Categorical":
                df[var] = df[var].fillna(missing_value_cat)
                df[var] = df[var].replace("--", missing_value_cat)
                df[var] = pd.Categorical(df[var])
        return df

    def PSI_calculation(Train, OOT, module, month):
        # psi def- Train, OOT, missing_value_num, cat_col, num_col, module, month
        # missing_ind def - Train, missing_value_num, missing_value_cat, cat_col, num_col
        psi_list = []
        feature_list = []
        list_psi = {}
        Train = missing_ind_convert_num(Train)
        OOT = missing_ind_convert_num(OOT)
        for var in Train.columns:
            if var_type(var) == "Numerical":
                psi_t = calculate_psi_num(Train[var], OOT[var])
                psi_list.append(psi_t)
                feature_list.append(var)
            elif var_type(var) == "Categorical":
                psi_t = calculate_psi_cat(Train[var], OOT[var])
                psi_list.append(psi_t)
                feature_list.append(var)

        list_psi["COL_MODULE"] = module.upper()
        list_psi["PSI_MONTH"] = month.upper()
        list_psi["PSI_VAR"] = feature_list
        list_psi["PSI"] = psi_list
        psi_df = pd.DataFrame(list_psi)
        return psi_df

    def get_data():
        sql_query = Get_query("MASTER_TABLE_GENERATION").get_gini_table
        data = pd.read_sql(sql_query, con=conn)
        if len(data) == 0:
            raise ValueError("Data shape not correct")
        return data

    def read_file(bucket_name, file_name):
        obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"]

    modules = [
        # ["sms_module", "SMS_PD", "IS_SMS_FEATURES_AVAILABLE"],
        ["transaction_module", "TRX_PD", "IS_LEDGER_FEATURES_AVAILABLE"],
        ["bureau_module", "BR_PD", "IS_BUREAU_FEATURES_AVAILBALE"],
        ["combination_module_xg", "CALIB_PD", None],
    ]

    oot_data = get_data()
    oot_data = oot_data.loc[oot_data["MODEL_TYPE"] == "XGBOOST", :]
    months = list(oot_data["MONTH_YEAR"].value_counts().keys().values)
    frames = pd.DataFrame()
    for value in modules:
        module = value[0]
        col = value[1]
        train_data = pd.read_csv(f"assets/psi/{module}_Train_data_score_model_xgb.csv")
        config_var = Variable.get("underwriting_dags_v2", deserialize_json=True)[
            f"{module}"
        ]
        missing_value_num = config_var["missing_value_num"]
        missing_value_cat = config_var["missing_value_cat"]
        if module != "combination_module_lr" and module != "combination_module_xg":
            col_check = value[2]
            input_path = config_var["input_path"]
            module_data = copy.deepcopy(
                oot_data[
                    (oot_data[f"{col}"].notnull()) & (oot_data[f"{col_check}"] == True)
                ]
            )
            feature_list = pd.read_csv(
                read_file(s3_bucket, input_path + f"KB_{module}_variables.csv")
            )
            cat_col = list(
                feature_list["variables"][feature_list["Type"] == "Categorical"]
            )
            num_col = list(
                feature_list["variables"][feature_list["Type"] == "Numerical"]
            )
        else:
            num_col = ["TRX_PD", "SMS_PD", "BR_PD"]
            cat_col = []
            module_data = copy.deepcopy(oot_data[(oot_data[f"{col}"].notnull())])
        num_col.append(col)
        module_psi = PSI_calculation(
            train_data,
            module_data,
            # missing_value_num,
            # cat_col,
            # num_col,
            module,
            month="OVERALL",
        )
        frames = frames.append(module_psi)
        for month in months:
            module_data_month = module_data.groupby(["MONTH_YEAR"])
            module_data_month = module_data_month.get_group(f"{month}")

            print(f"***************{module}***************")
            print(f"***************{module_data.shape}***************")
            print(f"***************{month}***************")
            print(f"***************{module_data_month.shape}***************")

            module_psi = PSI_calculation(
                train_data,
                module_data_month,
                # missing_value_num,
                # cat_col,
                # num_col,
                module,
                month=month,
            )
            frames = frames.append(module_psi)

    frames.reset_index(inplace=True, drop=True)
    # return frames
    truncate_table("psi_results_rule_add", "MASTER_TABLE_GENERATION".lower())
    write_to_snowflake(frames, "psi_results_rule_add", "MASTER_TABLE_GENERATION".lower())


calculate_PSI("MASTER_TABLE_GENERATION")
