import logging
import boto3
import pandas as pd
import snowflake.connector
import logging
from datetime import datetime
from airflow.models import Variable
from uw_utility_functions_v2 import write_to_snowflake, get_connector
import os

config_var = Variable.get("underwriting_dags_v2", deserialize_json=True)[
    "bureau_module"
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
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.{identifier}_{dataset_name}_dag_version_two"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return


feature_list = pd.read_csv(
    read_file(s3_bucket, model_path + "KB_bureau_module_variables.csv")
)

config = Variable.get("underwriting_dags_v2", deserialize_json=True)

conn = get_connector()
cur = conn.cursor()


def xgboost_model_prediction(dataset_name, **kwargs):
    import pickle
    import numpy as np
    import statsmodels.api as sm
    from uw_sql_queries_v2 import Get_query

    # start_date = kwargs["ti"].xcom_pull(key="start_date")
    start_date = "2022-08-01"
    # end_date = datetime.now().strftime("%Y-%m-%d")
    end_date = "2023-02-21"
    print(f"*********************** start_date: {start_date}***********************")
    print(f"*********************** end_date: {end_date} ***********************")

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
                df[var] = df[var].replace("--", missing_value_cat)
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
    data["logodds_score"] = np.log(data["pred_train"] / (1 - data["pred_train"]))

    model_xgb_calib = pickle.loads(
        s3.Bucket(s3_bucket)
        .Object(f"{model_path}Model_LR_calibration_xgb.pkl")
        .get()["Body"]
        .read()
    )
    data["pred_train_xgb"] = model_xgb_calib.predict(
        sm.add_constant(data["logodds_score"], has_constant="add")
    )
    truncate_table("result_xgb", dataset_name.lower())
    write_to_snowflake(data, "result_xgb", dataset_name.lower())
    logging.info("Finished Model prediction 2")
    cur.close()
    conn.close()


xgboost_model_prediction("KB_BUREAU_MODULE")
