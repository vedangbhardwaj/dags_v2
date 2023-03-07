import pandas as pd
import snowflake.connector
import json
import copy
import boto3
import logging
from airflow.models import Variable
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

sql_cmd = """ 
select 
    ID,
    'XGBOOST' MODEL_TYPE,
    EXPIRE_AT,
    CREATED_AT,
    UPDATED_AT,
    LOAN_APPLICATION_ID,
    UID,
    USER_ID,
    TYPE,
    STATUS,
    LOAN_TYPE,
    LENDER,
    METADATA:scoringServiceInput MODEL_INPUT,
    METADATA:scoringServiceOutput MODEL_OUTPUT,
    METADATA:reRenewalData:IS_RENEWAL::BOOLEAN IS_RENEWAL,
    METADATA:reVendorInsightsData:X_SELL_SCORE X_SELL_SCORE,
    METADATA:reVendorInsightsData:FIS_SCORE FIS_SCORE,
    METADATA:riskBucket::TEXT LENDER_RISK_BUCKET,
    METADATA:scoringServiceOutput:model_config:model_version::TEXT MODEL_VERSION,
    METADATA:scoringServiceOutput:raw_output:COMBINATION_TYPE::TEXT COMBINATION_TYPE,
    METADATA:scoringServiceOutput:raw_output:Risk_bands::TEXT MODEL_RISK_BUCKET,
    METADATA:scoringServiceOutput:output:UserID::TEXT UserID,
    METADATA:scoringServiceOutput:raw_output:Br_PD::FLOAT BR_PD,
    METADATA:scoringServiceOutput:raw_output:Calib_PD::FLOAT CALIB_PD,
    METADATA:scoringServiceOutput:raw_output:Combined_PD::FLOAT COMBINED_PD,
    METADATA:scoringServiceOutput:raw_output:Combined_logodds::FLOAT COMBINED_LOGODDS,
    METADATA:scoringServiceOutput:raw_output:IS_BR_SUFF::TEXT IS_BR_SUFF,
    METADATA:scoringServiceOutput:raw_output:IS_SMS_SUFF::TEXT IS_SMS_SUFF,
    METADATA:scoringServiceOutput:raw_output:IS_TRX_SUFF::TEXT IS_TRX_SUFF,
    METADATA:scoringServiceOutput:raw_output:SMS_PD::FLOAT SMS_PD,
    METADATA:scoringServiceOutput:raw_output:Trx_PD::FLOAT TRX_PD,
    METADATA:scoringServiceOutput:raw_output:logodds_br::FLOAT LOGODDS_BR,
    METADATA:scoringServiceOutput:raw_output:logodds_trx::FLOAT LOGODDS_TRX,
    METADATA:scoringServiceOutput:raw_output:pred_br::FLOAT PRED_BR,
    METADATA:scoringServiceOutput:raw_output:pred_sms::FLOAT PRED_SMS,
    METADATA:scoringServiceOutput:raw_output:pred_trx::FLOAT PRED_TRX

from APP_BACKEND.LOAN_SERVICE_PROD.PUBLIC_LOAN_OFFER_REQUEST_VW
where METADATA:scoringServiceOutput is not NULL ;
"""

def truncate_table(identifier, dataset_name):
    sql_cmd = f"TRUNCATE TABLE IF EXISTS analytics.kb_analytics.{identifier}_{dataset_name}_dag_version_two"
    try:
        cur.execute(sql_cmd)
    except Exception as error:
        logging.info(f"Error on truncate_table:{error}")
    return

def build_master_dict(data):
    for row in range(len(data)):
        json_object = json.loads(data["MODEL_INPUT"].iloc[row])
        comb = {}
        comb["CREATED_AT"] = data.iloc[row]["CREATED_AT"].strftime("%Y-%m-%d")
        comb["USER_ID"] = data.iloc[row]["USER_ID"]
        comb["LOAN_APPLICATION_ID"] = data.iloc[row]["LOAN_APPLICATION_ID"]
        comb["CALIB_PD"] = data.iloc[row]["CALIB_PD"]

        for module, val in json_object["features"].items():
            if len(json_object["features"][f"{module}"]) > 0:
                json_object["features"][f"{module}"]["CREATED_AT"] = data.iloc[row]["CREATED_AT"].strftime("%Y-%m-%d")
                json_object["features"][f"{module}"]["USER_ID"] = data.iloc[row]["USER_ID"]
                json_object["features"][f"{module}"]["LOAN_APPLICATION_ID"] = data.iloc[row]["LOAN_APPLICATION_ID"]

                if module.upper() == "SMS" and "SMS_PD" in data.iloc[row].keys():
                    json_object["features"][f"{module}"]["SMS_PD"] = data.iloc[row]["SMS_PD"]
                    # combination module
                    comb["SMS_PD"] = data.iloc[row]["SMS_PD"]
                
                elif module.upper() == "LEDGER" and "TRX_PD" in data.iloc[row].keys():
                    json_object["features"][f"{module}"]["TRX_PD"] = data.iloc[row]["TRX_PD"]
                    #combination module
                    comb["TRX_PD"] = data.iloc[row]["TRX_PD"]
                
                elif module.upper() == "BUREAU" and "BR_PD" in data.iloc[row].keys():
                    json_object["features"][f"{module}"]["BR_PD"] = data.iloc[row]["BR_PD"]
                    #combination module
                    comb["BR_PD"] = data.iloc[row]["BR_PD"]
                
                master_dict[f"{module}"].append(val)
        
        # combination module
        master_dict["combination_module_xg"].append(comb)
        
    master_dict["sms_module"] = master_dict.pop("sms")
    master_dict["transaction_module"] = master_dict.pop("ledger")
    master_dict["bureau_module"] = master_dict.pop("bureau")
    return master_dict

def build_dataframe(module, get_master_dict):
    df = pd.DataFrame.from_dict(get_master_dict[f"{module}"], orient="columns")
    df["MONTH_YEAR"] = pd.to_datetime(data["CREATED_AT"]).dt.strftime("%b-%Y")
    return df

data = pd.read_sql(sql_cmd, con=conn)
master_dict = {"ledger": [], "sms": [], "bureau": [], "activity": [], "combination_module_xg": []}
get_master_dict = build_master_dict(data)

def calculate_PSI():
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
        for var in OOT.columns:
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

    def read_file(bucket_name, file_name):
        obj = s3.meta.client.get_object(Bucket=bucket_name, Key=file_name)
        return obj["Body"]

    modules = [
        # ["sms_module", "SMS_PD", "IS_SMS_FEATURES_AVAILABLE"],
        ["transaction_module", "TRX_PD", "IS_LEDGER_FEATURES_AVAILABLE"],
        ["bureau_module", "BR_PD", "IS_BUREAU_FEATURES_AVAILBALE"],
        ["combination_module_xg", "CALIB_PD", None],
    ]

    frames = pd.DataFrame()
    for value in modules:
        module = value[0]
        col = value[1]

        oot_data = build_dataframe(f"{module}", get_master_dict)
        months = list(oot_data["MONTH_YEAR"].value_counts().keys().values)

        train_data = pd.read_csv(
            read_file(s3_bucket, "underwriting_assets/psi/assets/" + f"{module}_Train_data_score_model_xgb.csv")
        )
        config_var = Variable.get("underwriting_dags_v2", deserialize_json=True)[
            f"{module}"
        ]
        missing_value_num = config_var["missing_value_num"]
        missing_value_cat = config_var["missing_value_cat"]
        if module != "combination_module_lr" and module != "combination_module_xg":
            input_path = config_var["input_path"]
            module_data = copy.deepcopy(
                oot_data[
                    (oot_data[f"{col}"].notnull()) 
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
    truncate_table("psi_result", "realtime_dashboard".lower())
    write_to_snowflake(
        frames, "psi_result", "realtime_dashboard".lower()
    )



#######

