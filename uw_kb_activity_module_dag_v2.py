import logging
import boto3
import pandas as pd
import snowflake.connector
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from uw_utility_functions_v2 import write_to_snowflake, get_connector
import os

config_var = Variable.get("underwriting_dags_v2", deserialize_json=True)[
    "activity_module"
]

missing_value_num = config_var["missing_value_num"]
missing_value_cat = config_var["missing_value_cat"]
IV_threshold = config_var["IV_threshold"]  ### threshold for IV (IV should be accepted
var_threshold = config_var["var_threshold"]
ID_cols = config_var["ID_cols"]
input_path = config_var["input_path"]
data_path = config_var["data_path"]
model_path = config_var["model_path"]

config = Variable.get("underwriting_dags_v2", deserialize_json=True)

s3 = boto3.resource("s3")
s3_bucket = config["s3_bucket"]


def read_file(bucket_name, file_name):
    obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
    return obj["Body"]


def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.airflow_demo_write_{identifier}_{dataset_name}"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return


conn = get_connector()
cur = conn.cursor()


def getting_data(dataset_name, **kwargs):
    from uw_sql_queries_v2 import Get_query

    start_date = kwargs["ti"].xcom_pull(key="start_date")
    end_date = datetime.now().strftime("%Y-%m-%d")

    def read_file(bucket_name, file_name):
        obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"]

    feature_list = pd.read_csv(
        read_file(s3_bucket, input_path + "KB_activity_module_variables.csv")
    )

    def get_raw_data(start_date, end_date):
        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    cat_col = list(feature_list["variables"][feature_list["Type"] == "Categorical"])
    num_col = list(feature_list["variables"][feature_list["Type"] == "Numerical"])

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

    data = get_raw_data(start_date, end_date)
    data = missing_ind_convert_num(data)
    truncate_table("transformed", dataset_name.lower())
    write_to_snowflake(data, "transformed", dataset_name.lower())
    # # cur.close()
    return


def woe_calculation(dataset_name):
    import scorecardpy as sc
    from uw_sql_queries_v2 import Get_query

    def get_raw_data():
        sql_query = Get_query(dataset_name).get_transformed_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    def woe_Apply(data, final_bin1):
        new_bin = final_bin1[final_bin1.columns[0:13]]
        data_w = sc.woebin_ply(data, new_bin)
        data_w_features = data_w.filter(regex="_woe$", axis=1)
        data_woe = pd.concat((data[ID_cols], data_w_features), axis=1)
        print(data_woe.columns.to_list())
        return data_woe

    data = get_raw_data()
    Final_bin_gini = pd.read_csv(
        read_file(s3_bucket, data_path + "Final_bin_gini_performance.csv")
    )
    data_woe = woe_Apply(data, Final_bin_gini)
    truncate_table("transformed_woe", dataset_name.lower())
    write_to_snowflake(data_woe, "transformed_woe", dataset_name.lower())
    # # cur.close()
    # # conn.close()


def model_prediction(dataset_name, **kwargs):
    import pickle

    import statsmodels.api as sm

    from uw_sql_queries_v2 import Get_query

    start_date = kwargs["ti"].xcom_pull(key="start_date")
    end_date = datetime.now().strftime("%Y-%m-%d")
    print(f"*********************** start_date: {start_date}***********************")
    print(f"*********************** end_date: {end_date} ***********************")

    def get_raw_data():
        sql_query = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        data = pd.read_sql(sql_query, con=conn)
        return data

    def get_data_woe():
        sql_query = Get_query(dataset_name).get_transformed_woe_data
        data = pd.read_sql(sql_query, con=conn)
        return data

    data = get_raw_data()
    data_woe = get_data_woe()

    model_perf2 = pd.read_csv(read_file(s3_bucket, data_path + "Model_selected.csv"))
    Top_models = [0]
    j = 0

    # for j in Top_models:
    Final_model_vars = list(model_perf2["variable"][model_perf2["Model_no"] == j])
    Final_model_vars = Final_model_vars[1 : len(Final_model_vars)]
    Final_model_vars.append("BRE_RUN_ID")
    Final_model_vars = [str(x).upper() for x in Final_model_vars]

    Final_scoring_data = data[ID_cols].merge(
        data_woe[Final_model_vars], on="BRE_RUN_ID", how="left"
    )
    Final_model_vars.pop()  # removing added BRE_RUN_ID here
    pred_data = Final_scoring_data[Final_model_vars]
    pred_data = sm.add_constant(pred_data, has_constant="add")

    pickled_model = pickle.loads(
        s3.Bucket(s3_bucket).Object(f"{model_path}Model_{j}.pkl").get()["Body"].read()
    )

    # Final model prediction
    Final_scoring_data["pred_train"] = pickled_model.predict(pred_data)
    truncate_table("result", dataset_name.lower())
    write_to_snowflake(Final_scoring_data, "result", dataset_name.lower())
    # cur.close()
    # conn.close()


def xgboost_model_prediction(dataset_name, **kwargs):
    import pickle
    import numpy as np
    import statsmodels.api as sm
    from uw_sql_queries_v2 import Get_query

    start_date = kwargs["ti"].xcom_pull(key="start_date")
    end_date = datetime.now().strftime("%Y-%m-%d")

    def read_file(bucket_name, file_name):
        obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"]

    feature_list = pd.read_csv(
        read_file(s3_bucket, input_path + "KB_activity_module_variables.csv")
    )

    def get_raw_data(start_date, end_date):
        sql_cmd = Get_query(dataset_name).get_raw_data.format(
            sd=start_date, ed=end_date
        )
        cur.execute(sql_cmd)
        df = pd.DataFrame(cur.fetchall())
        colnames = [desc[0] for desc in cur.description]
        df.columns = [i for i in colnames]
        return df

    cat_col = list(feature_list["variables"][feature_list["Type"] == "Categorical"])
    num_col = list(feature_list["variables"][feature_list["Type"] == "Numerical"])

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
        for var in df.columns:
            if var_type(var) == "Categorical":
                # df[var] = df[var].replace("--", missing_value_cat)
                df[var] = pd.Categorical(df[var])
        return df

    data = get_raw_data(start_date, end_date)
    data = missing_ind_convert_num(data)

    XGB_keep_var_list = pd.read_csv(
        read_file(s3_bucket, model_path + "XGBoost_feature_list.csv")
    )

    keep_var_list = list(XGB_keep_var_list["variables"])
    data1 = data[keep_var_list]

    pickled_model = pickle.loads(
        s3.Bucket(s3_bucket).Object(f"{model_path}Model_xgb.pkl").get()["Body"].read()
    )

    data["pred_train"] = pickled_model.predict_proba(data1)[:, 1]
    logging.info("Finished Model prediction")
    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.loads(
        s3.Bucket(s3_bucket)
        .Object(f"{model_path}Model_LR_calibration_xgb.pkl")
        .get()["Body"]
        .read()
    )

    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"])
    )
    truncate_table("result_xgb", dataset_name.lower())
    write_to_snowflake(data, "result_xgb", dataset_name.lower())
    logging.info("Finished Model prediction 2")
    # cur.close()
    # conn.close()
