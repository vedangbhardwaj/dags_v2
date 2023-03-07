import pandas as pd


class Get_query:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.get_raw_data = None
        self.get_transformed_data = None
        self.get_transformed_woe_data = None
        self.get_txn_data = None
        self.get_activity_data = None
        self.get_bureau_data = None
        self.get_policy_run_gen_table = None
        self.merge_master_table = None
        self.get_master_table = None
        # loan_id, ever11DPD_in_90days, ever15DPD_in_90days as BAD_FLAG, ever15DPD_in_60days,
        # ANALYTICS.KB_ANALYTICS.A4_UW_KB_Activity_data_v3 ; ANALYTICS.KB_ANALYTICS.A1_UW_model_lending_Bureau_base_v1
        #
        if self.dataset_name.strip().upper() == "KB_TXN_MODULE":
            self.get_raw_data = """
                select  
                    CUSTOMER_ID as user_id,
                    BRE_RUN_ID,
                    BRE_RUN_DATE as disbursed_date,
                    TOTAL_KHATAS,
                    KB_AGE,
                    TOTAL_CUSTOMERS,
                    NO_KB_CUSTOMERS,
                    DAYS_SINCE_FIRST_CUST_ADDED,
                    DAYS_SINCE_LAST_CUST_ADDED,
                    TOTAL_TRXNS,
                    TOTAL_TXN_MONTHS,
                    ACTIVE_KHATAS,
                    ACTIVE_DAYS,
                    DEBIT_TRXNS,
                    CREDIT_TRXNS,
                    DEBIT_AMNT,
                    CREDIT_AMNT,
                    TOTAL_TXN_AMNT,
                    DAYS_SINCE_FIRST_TXN,
                    DAYS_SINCE_LAST_TXN,
                    MONTHS_SINCE_FIRST_TXN,
                    MONTHS_SINCE_LAST_TXN,
                    ACTIVATION_AGE,
                    D_1_TXNS,
                    D_30_TXNS,
                    AVG_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_TRXNS_AMOUNT_L1MONTH_DAILY,
                    AVG_TRXNS_AMT_L2MONTH_DAILY,
                    AVG_TRXNS_AMT_L3MONTH_DAILY,
                    AVG_TRXNS_AMT_L6MONTH_DAILY,
                    AVG_TRXNS_AMT_L12MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_CREDIT_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L1MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L2MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L3MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L6MONTH_DAILY,
                    AVG_DEBIT_TRXNS_CNT_L12MONTH_DAILY,
                    AVG_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_TRXNS_CNT_L12MONTH_MONTHLY,
                    AVG_TRXNS_AMOUNT_L1MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L2MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L3MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L6MONTH_MONTHLY,
                    AVG_TRXNS_AMT_L12MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_CREDIT_TRXNS_CNT_L12MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L1MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L2MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L3MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L6MONTH_MONTHLY,
                    AVG_DEBIT_TRXNS_CNT_L12MONTH_MONTHLY,
                    TOTAL_TRXNS_CNT_3_BY_6,
                    TOTAL_TRXNS_CNT_3_BY_12,
                    round(RATIO_CNT_L1_L3_MNTH_DAILY,2) as RATIO_CNT_L1_L3_MNTH_DAILY,
                    round(RATIO_AMNT_L1_L3_MNTH_DAILY,2) as RATIO_AMNT_L1_L3_MNTH_DAILY,
                    RATIO_CNT_L1_L3_MNTH_MONTHLY,
                    RATIO_AMNT_L1_L3_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_1_MNTH_DAILY,
                    AVG_TRAN_SIZE_2_MNTH_DAILY,
                    AVG_TRAN_SIZE_3_MNTH_DAILY,
                    AVG_TRAN_SIZE_1_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_2_MNTH_MONTHLY,
                    AVG_TRAN_SIZE_3_MNTH_MONTHLY,
                    TOTAL_COUNTERPARTY_TRXNS,
                    TOTAL_COUNTERPARTY_TXN_AMT,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_DAILY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_DAILY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY,
                    AVG_TXN_CNT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_1MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_2MNTH_MONTHLY,
                    AVG_TXN_AMT_WITH_KB_COUNTERPARTY_USERS_3MNTH_MONTHLY,
                    CREDIT_EXTENDED_TOTAL,
                    CREDIT_RECOVERED,
                    AMT_RECOVERED_0TO30,
                    AMT_RECOVERED_31TO60,
                    AMT_RECOVERED_61TO90,
                    PARTIAL_SETTLED_TRXNS,
                    FULL_SETTLED_TRXNS,
                    UNSETTLED_TRXNS,
                    CREDIT_EXTENDED_3MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_6MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_12MNTH_AVG_DAILY,
                    CREDIT_EXTENDED_OVERALL_AVG_DAILY,
                    CREDIT_RECOVERED_3MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_6MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_12MNTH_AVG_DAILY,
                    CREDIT_RECOVERED_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_0TO30_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_0TO30_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_31TO60_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_31TO60_OVERALL_AVG_DAILY,
                    AMT_RECOVERED_61TO90_3MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_6MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_12MNTH_AVG_DAILY,
                    AMT_RECOVERED_61TO90_OVERALL_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_3MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_6MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_12MNTH_AVG_DAILY,
                    PARTIAL_SETTLED_TRXNS_OVERALL_AVG_DAILY,
                    FULL_SETTLED_TRXNS_3MNTH_AVG_DAILY,
                    FULL_SETTLED_TRXNS_6MNTH_AVG_DAILY,
                    FULL_SETTLED_TRXNS_12MNTH_AVG_DAILY,
                    round(FULL_SETTLED_TRXNS_OVERALL_AVG_DAILY,2) as FULL_SETTLED_TRXNS_OVERALL_AVG_DAILY,
                    UNSETTLED_TRXNS_3MNTH_AVG_DAILY,
                    UNSETTLED_TRXNS_6MNTH_AVG_DAILY,
                    UNSETTLED_TRXNS_12MNTH_AVG_DAILY,
                    round(UNSETTLED_TRXNS_OVERALL_AVG_DAILY,2) as UNSETTLED_TRXNS_OVERALL_AVG_DAILY,
                    CREDIT_EXTENDED_3MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_6MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_12MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_OVERALL_AVG_MONTHLY,
                    CREDIT_RECOVERED_3MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_6MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_12MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERED_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_0TO30_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_31TO60_OVERALL_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_3MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_6MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_12MNTH_AVG_MONTHLY,
                    AMT_RECOVERED_61TO90_OVERALL_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    PARTIAL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    round(PARTIAL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,2) as PARTIAL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    FULL_SETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    UNSETTLED_TRXNS_3MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_6MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_12MNTH_AVG_MONTHLY,
                    UNSETTLED_TRXNS_OVERALL_AVG_MONTHLY,
                    CREDIT_EXTENDED_LAST_MONTH_VALUE,
                    CREDIT_EXTENDED_3_BY_6,
                    CREDIT_RECOVERED_3_BY_6,
                    AMT_RECOVERED_0TO30_3_BY_6,
                    AMT_RECOVERED_31TO60_3_BY_6,
                    AMT_RECOVERED_61TO90_3_BY_6,
                    PARTIAL_SETTLED_TRXNS_3_BY_6,
                    FULL_SETTLED_TRXNS_3_BY_6,
                    UNSETTLED_TRXNS_3_BY_6,
                    CREDIT_EXTENDED_3_BY_12,
                    CREDIT_RECOVERED_3_BY_12,
                    AMT_RECOVERED_0TO30_3_BY_12,
                    AMT_RECOVERED_31TO60_3_BY_12,
                    AMT_RECOVERED_61TO90_3_BY_12,
                    PARTIAL_SETTLED_TRXNS_3_BY_12,
                    FULL_SETTLED_TRXNS_3_BY_12,
                    UNSETTLED_TRXNS_3_BY_12,
                    CREDIT_RECOVERY_RATIO_3_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_6_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_12_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_DAILY,
                    CREDIT_RECOVERY_RATIO_3_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_6_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_12_MNTH_AVG_MONTHLY,
                    CREDIT_RECOVERY_RATIO_OVERALL_MNTH_AVG_MONTHLY,
                    CREDIT_EXTENDED_3MNTH_SUM,
                    CREDIT_RECOVERED_3MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_3_MNTH,
                    CREDIT_EXTENDED_6MNTH_SUM,
                    CREDIT_RECOVERED_6MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_6_MNTH,
                    CREDIT_EXTENDED_12MNTH_SUM,
                    CREDIT_RECOVERED_12MNTH_SUM,
                    CREDIT_RECOVERY_RATIO_12_MNTH,
                    CREDIT_RECOVERY_RATIO_3_BY_6,
                    CREDIT_RECOVERY_RATIO_3_BY_12,
                    IS_LEDGER_FEATURES_AVAILABLE
                from ANALYTICS.LENDING_PROD.LENDING_FACT
                where disbursed_date>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                and (DAYS_SINCE_LAST_TXN<=90 and DAYS_SINCE_LAST_TXN is not NULL)
                """
            # analytics.kb_analytics.A1_UW_model_lending_transaction_base_v1
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_txn_module
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_txn_module
                    """
        if self.dataset_name.strip().upper() == "KB_ACTIVITY_MODULE":

            self.get_raw_data = """
                    select  
                        CUSTOMER_ID as user_id
                        ,BRE_RUN_ID
                        ,BRE_RUN_DATE as disbursed_date
                        ,LAST_1_MONTH_CUSTOMERS
                        ,LAST_2_MONTH_CUSTOMERS
                        ,LAST_3_MONTH_CUSTOMERS
                        ,RATIO_OF_CUSTOMERS_ADDED_L1_L3_MNTH
                        ,DELETED_CUSTOMERS
                        ,LAST_1_MONTH_BOOKS
                        ,LAST_2_MONTH_BOOKS
                        ,LAST_3_MONTH_BOOKS
                        ,RATIO_OF_BOOKS_ADDED_L1_L3_MNTH
                        ,DELETED_BOOKS
                        ,TOTAL_APPS
                        ,BUSINESS_APPS
                        ,FINANCE_APPS
                        ,BUSINESS_TOTALAPPS_PROPORTION
                        ,FINANCE_TOTALAPPS_PROPORTION
                        ,DEVICE_BRAND_GD
                        ,DEVICE_CARRIER_GROUP
                        ,PRICE
                        ,Business_Card_Flag
                        ,IS_ACTIVITY_FEATURES_AVAILABLE
                    from ANALYTICS.LENDING_PROD.LENDING_FACT
                    where disbursed_date>=date('{sd}')
                    and date(disbursed_date)<=date('{ed}')
                """
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_activity_module 
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_activity_module 
                    """
        if self.dataset_name.strip().upper() == "KB_BUREAU_MODULE":
            # SQL Query - getting raw data:
            self.get_raw_data = """
                select  
                    CUSTOMER_ID as user_id,
                    BRE_RUN_ID,
                    BRE_RUN_DATE as disbursed_date,
                    BUREAUSCORE,
                    CREDITACCOUNTTOTAL,
                    CREDITACCOUNTACTIVE,
                    CREDITACCOUNTDEFAULT,
                    CREDITACCOUNTCLOSED,
                    CADSUITFILEDCURRENTBALANCE,
                    OUTSTANDING_BALANCE_SECURED,
                    OUTSTANDING_BALANCE_SECURED_PERCENTAGE,
                    OUTSTANDING_BALANCE_UNSECURED,
                    OUTSTANDING_BALANCE_UNSECURED_PERCENTAGE,
                    OUTSTANDING_BALANCE_ALL,
                    ACCOUNT_TYPE_COUNT,
                    CREDIT_CARDS_COUNT,
                    SECURED_ACCOUNT_COUNT,
                    UNSECURED_ACCOUNT_COUNT,
                    CREDIT_LIMIT_AMOUNT_MAX,
                    CREDIT_LIMIT_AMOUNT_AVG,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_SUM,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_AVG,
                    HIGHEST_CREDIT_OR_ORIGINAL_LOAN_AMOUNT_MAX,
                    TERMS_DURATION_AVG,
                    TERMS_DURATION_MAX,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_SUM,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_AVG,
                    SCHEDULED_MONTHLY_PAYMENT_AMOUNT_MAX,
                    ACCOUNT_STATUS_ACTIVE,
                    ACCOUNT_STATUS_CLOSED,
                    CURRENT_BALANCE_SUM,
                    CURRENT_BALANCE_AVG,
                    CURRENT_BALANCE_MAX,
                    CURRENT_BALANCE_MIN,
                    AMOUNT_PAST_DUE_SUM,
                    AMOUNT_PAST_DUE_AVG,
                    AMOUNT_PAST_DUE_MAX,
                    ORIGINAL_CHARGE_OFF_AMOUNT_SUM,
                    ORIGINAL_CHARGE_OFF_AMOUNT_AVG,
                    ORIGINAL_CHARGE_OFF_AMOUNT_MAX,
                    DAYS_SINCE_FIRST_DELINQUENCY,
                    DAYS_SINCE_LAST_PAYMENT,
                    SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS,
                    SUITFILED_WILFULDEFAULT,
                    VALUE_OF_CREDITS_LAST_MONTH_SUM,
                    VALUE_OF_CREDITS_LAST_MONTH_AVG,
                    VALUE_OF_CREDITS_LAST_MONTH_MAX,
                    SETTLEMENT_AMOUNT_SUM,
                    SETTLEMENT_AMOUNT_AVG,
                    SETTLEMENT_AMOUNT_MAX,
                    VALUE_OF_COLLATERAL_SUM,
                    VALUE_OF_COLLATERAL_AVG,
                    VALUE_OF_COLLATERAL_MAX,
                    TYPE_OF_COLLATERAL,
                    WRITTEN_OFF_AMT_TOTAL_SUM,
                    WRITTEN_OFF_AMT_TOTAL_AVG,
                    WRITTEN_OFF_AMT_TOTAL_MAX,
                    WRITTEN_OFF_AMT_PRINCIPAL_SUM,
                    WRITTEN_OFF_AMT_PRINCIPAL_AVG,
                    WRITTEN_OFF_AMT_PRINCIPAL_MAX,
                    RATE_OF_INTEREST_AVG,
                    RATE_OF_INTEREST_MAX,
                    RATE_OF_INTEREST_MIN,
                    REPAYMENT_TENURE_AVG,
                    REPAYMENT_TENURE_MAX,
                    DAYS_SINCE_LAST_DEFAULT_STATUS,
                    DAYS_SINCE_LAST_LITIGATION_STATUS,
                    DAYS_SINCE_LAST_WRITE_OFF_STATUS,
                    MAX_DPD_LAST_1_MONTH,
                    MAX_DPD_LAST_3_MONTHS,
                    MAX_DPD_LAST_6_MONTHS,
                    MAX_DPD_LAST_9_MONTHS,
                    MAX_DPD_LAST_12_MONTHS,
                    MAX_DPD_LAST_24_MONTHS,
                    MAX_DPD_LAST_1_MONTH_ACTIVE,
                    MAX_DPD_LAST_3_MONTHS_ACTIVE,
                    MAX_DPD_LAST_6_MONTHS_ACTIVE,
                    MAX_DPD_LAST_9_MONTHS_ACTIVE,
                    MAX_DPD_LAST_12_MONTHS_ACTIVE,
                    MAX_DPD_LAST_24_MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_3MONTHS,
                    NO_OF_TIMES_30_DPD_6MONTHS,
                    NO_OF_TIMES_30_DPD_9MONTHS,
                    NO_OF_TIMES_30_DPD_12MONTHS,
                    NO_OF_TIMES_30_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_30_DPD_12MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_3MONTHS,
                    NO_OF_TIMES_60_DPD_6MONTHS,
                    NO_OF_TIMES_60_DPD_9MONTHS,
                    NO_OF_TIMES_60_DPD_12MONTHS,
                    NO_OF_TIMES_60_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_60_DPD_12MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_3MONTHS,
                    NO_OF_TIMES_90_DPD_6MONTHS,
                    NO_OF_TIMES_90_DPD_9MONTHS,
                    NO_OF_TIMES_90_DPD_12MONTHS,
                    NO_OF_TIMES_90_DPD_3MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_6MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_9MONTHS_ACTIVE,
                    NO_OF_TIMES_90_DPD_12MONTHS_ACTIVE,
                    MONTHS_SINCE_30_DPD,
                    MONTHS_SINCE_60_DPD,
                    MONTHS_SINCE_90_DPD,
                    MONTHS_SINCE_30_DPD_ACTIVE,
                    MONTHS_SINCE_60_DPD_ACTIVE,
                    MONTHS_SINCE_90_DPD_ACTIVE,
                    TOTALCAPSLAST7DAYS,
                    TOTALCAPSLAST30DAYS,
                    TOTALCAPSLAST90DAYS,
                    TOTALCAPSLAST180DAYS,
                    CAPSLAST7DAYS,
                    CAPSLAST30DAYS,
                    CAPSLAST90DAYS,
                    CAPSLAST180DAYS,
                    RESIDENCE_CODE_NON_NORMALIZED,
                    AGE_OF_OLDEST_ACCOUNT_DAYS,
                    ACCOUNT_OPEN_1MONTH,
                    ACCOUNT_OPEN_2MONTHS,
                    ACCOUNT_OPEN_3MONTHS,
                    ACCOUNT_OPEN_6MONTHS,
                    ACCOUNT_OPEN_12MONTHS,
                    MAX_DPD_LAST_1_MONTH_SECURED,
                    MAX_DPD_LAST_3_MONTHS_SECURED,
                    MAX_DPD_LAST_6_MONTHS_SECURED,
                    MAX_DPD_LAST_9_MONTHS_SECURED,
                    MAX_DPD_LAST_12_MONTHS_SECURED,
                    MAX_DPD_LAST_24_MONTHS_SECURED,
                    MAX_DPD_LAST_1_MONTH_UNSECURED,
                    MAX_DPD_LAST_3_MONTHS_UNSECURED,
                    MAX_DPD_LAST_6_MONTHS_UNSECURED,
                    MAX_DPD_LAST_9_MONTHS_UNSECURED,
                    MAX_DPD_LAST_12_MONTHS_UNSECURED,
                    MAX_DPD_LAST_24_MONTHS_UNSECURED,
                    PAYMENT_WITHOUT_DPD_LAST_1_MONTH,
                    PAYMENT_WITHOUT_DPD_LAST_3_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_6_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_9_MONTHS,
                    PAYMENT_WITHOUT_DPD_LAST_12_MONTHS,
                    IS_BUREAU_FEATURES_AVAILBALE
                from  ANALYTICS.LENDING_PROD.LENDING_FACT
                where date(disbursed_date)>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                and IS_BUREAU_FEATURES_AVAILBALE = True
                """
            # SQL Query - getting transformed data:
            self.get_transformed_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_kb_bureau_module 
                    """
            # SQL Query - getting transformed WOE applied data:
            self.get_transformed_woe_data = """
                    SELECT * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_transformed_woe_kb_bureau_module 
                    """

        if self.dataset_name.strip().upper() == "KB_SMS_MODULE":
            # self.get_raw_data = """
            #     select * from ANALYTICS.KB_ANALYTICS.TEMP_SMS_TABLE_RAW_DATA;
            # """
            self.get_raw_data = """
                select
                    CUSTOMER_ID as USER_ID,
                    BRE_RUN_ID,
                    DEVICE_ID,
                    BRE_RUN_DATE as DISBURSED_DATE,
                    RATIO_NUM_DEBIT_TXNS_100_TO_500_NUM_CREDIT_TXNS_100_TO_500_LAST30D,
                    TOTAL_DEBIT_AMOUNT_UPI_150TO180D,
                    RATIO_NUM_DEBIT_TXNS_100_TO_500_LAST30D_VS_30TO60D,
                    RATIO_TOTAL_DEBIT_COUNT_UPI_LAST30D_VS_120TO150D,
                    AVG_DEBIT_AMOUNT_LAST30D,
                    RATIO_NUM_CREDIT_TXNS_500_TO_2000_LAST30D_VS_120TO150D,
                    RATIO_MIN_AVAILABLE_BALANCE_LAST30D_VS_60TO90D,
                    RATIO_NUM_DEBIT_TXNS_2000_TO_5000_LAST30D_VS_30TO60D,
                    NUM_DEBIT_TXNS_5000_TO_10000_LAST30D,
                    RATIO_TOTAL_CREDIT_COUNT_BANK_TRANSFER_LAST30D_VS_30TO60D,
                    RATIO_AVG_DEBIT_AMOUNT_AVG_CDT_AMT_LAST30D,
                    RATIO_TOTAL_DEBIT_AMOUNT_TOTAL_CREDIT_AMOUNT_90TO120D,
                    RATIO_AVG_AVAILABLE_BALANCE_LAST30D_VS_120TO150D,
                    NUM_CREDIT_TXNS_GREATERTHAN_10000_30TO60D,
                    RATIO_TOTAL_CREDIT_COUNT_UPI_LAST30D_VS_120TO150D,
                    TOTAL_DEBIT_AMOUNT_UPI_30TO60D,
                    MIN_DEBIT_AMOUNT_120TO150D,
                    TOTAL_CREDIT_AMOUNT_LAST30D,
                    RATIO_MIN_AVAILABLE_BALANCE_LAST30D_VS_120TO150D,
                    AVG_CDT_AMT_90TO120D,
                    MIN_AVAILABLE_BALANCE_LAST30D,
                    RATIO_NUM_DEBIT_TXNS_500_TO_2000_LAST30D_VS_30TO60D,
                    RATIO_TOTAL_DEBIT_COUNT_UPI_TOTAL_CREDIT_COUNT_UPI_120TO150D,
                    MAX_AVAILABLE_BALANCE_90TO120D,
                    RATIO_NUM_CREDIT_TXNS_LESSTHAN_100_LAST30D_VS_60TO90D,
                    MAX_AVAILABLE_BALANCE_60TO90D,
                    TOTAL_DEBIT_COUNT_UPI_60TO90D,
                    NUM_DEBIT_TXNS_GREATERTHAN_10000_LAST30D,
                    TOTAL_CREDIT_AMOUNT_UPI_60TO90D,
                    RATIO_TOTAL_DEBIT_AMOUNT_TOTAL_CREDIT_AMOUNT_LAST30D,
                    RATIO_TOTAL_DEBIT_AMOUNT_LAST30D_VS_60TO90D,
                    NUM_DEBIT_TXNS_LESSTHAN_100_90TO120D,
                    RATIO_NUM_DEBIT_TXNS_5000_TO_10000_NUM_CREDIT_TXNS_5000_TO_10000_LAST30D,
                    NUM_CREDIT_TXNS_LAST30D,
                    RATIO_NUM_DEBIT_TXNS_NUM_CREDIT_TXNS_LAST30D,
                    RATIO_TOTAL_CREDIT_AMOUNT_UPI_LAST30D_VS_30TO60D,
                    NUM_CREDIT_TXNS_90TO120D,
                    RATIO_NUM_DEBIT_TXNS_5000_TO_10000_NUM_CREDIT_TXNS_5000_TO_10000_90TO120D,
                    TOTAL_DEBIT_AMOUNT_LAST30D,
                    MAX_DEBIT_AMOUNT_LAST30D,
                    AVG_DEBIT_AMOUNT_60TO90D,
                    RATIO_NUM_CREDIT_TXNS_100_TO_500_LAST30D_VS_30TO60D,
                    MIN_AVAILABLE_BALANCE_120TO150D,
                    AVG_AVAILABLE_BALANCE_30TO60D,
                    MAX_DEBIT_AMOUNT_120TO150D,
                    RATIO_TOTAL_CREDIT_AMOUNT_LAST30D_VS_30TO60D,
                    RATIO_AVG_DEBIT_TXNS_AVG_CREDIT_TXNS_LAST30D,
                    AVG_CDT_AMT_60TO90D,
                    NUM_CREDIT_TXNS_500_TO_2000_30TO60D,
                    RATIO_NUM_CREDIT_TXNS_LESSTHAN_100_LAST30D_VS_30TO60D,
                    RATIO_MIN_CDT_AMT_LAST30D_VS_30TO60D,
                    NUM_DEBIT_TXNS_LAST30D,
                    MAX_AVAILABLE_BALANCE_LAST30D,
                    TOTAL_CREDIT_AMOUNT_UPI_LAST30D,
                    RATIO_NUM_CREDIT_TXNS_500_TO_2000_LAST30D_VS_30TO60D,
                    NUM_CREDIT_TXNS_100_TO_500_LAST30D,
                    TOTAL_DEBIT_AMOUNT_60TO90D,
                    AVG_AVAILABLE_BALANCE_60TO90D,
                    RATIO_TOTAL_DEBIT_COUNT_UPI_TOTAL_CREDIT_COUNT_UPI_90TO120D,
                    RATIO_MIN_DEBIT_AMOUNT_LAST30D_VS_30TO60D,
                    RATIO_TOTAL_DEBIT_COUNT_UPI_TOTAL_CREDIT_COUNT_UPI_LAST30D,
                    AVG_AVAILABLE_BALANCE_LAST30D,
                    NUM_DEBIT_TXNS_5000_TO_10000_150TO180D,
                    RATIO_AVG_CDT_AMT_LAST30D_VS_30TO60D,
                    TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_150TO180D,
                    AVG_DEBIT_AMOUNT_120TO150D,
                    RATIO_NUM_DEBIT_TXNS_LESSTHAN_100_NUM_CREDIT_TXNS_LESSTHAN_100_90TO120D,
                    RATIO_MIN_AVAILABLE_BALANCE_LAST30D_VS_30TO60D,
                    TOTAL_DEBIT_AMOUNT_UPI_60TO90D,
                    RATIO_NUM_CREDIT_TXNS_LAST30D_VS_30TO60D,
                    RATIO_MAX_AVAILABLE_BALANCE_LAST30D_VS_30TO60D,
                    MAX_AVAILABLE_BALANCE_150TO180D,
                    RATIO_MIN_AVAILABLE_BALANCE_LAST30D_VS_90TO120D,
                    RATIO_NUM_DEBIT_TXNS_2000_TO_5000_NUM_CREDIT_TXNS_2000_TO_5000_120TO150D,
                    RATIO_TOTAL_DEBIT_AMOUNT_UPI_TOTAL_CREDIT_AMOUNT_UPI_LAST30D,
                    RATIO_TOTAL_CREDIT_COUNT_UPI_LAST30D_VS_60TO90D,
                    RATIO_TOTAL_CREDIT_AMOUNT_UPI_LAST30D_VS_90TO120D,
                    TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_LAST30D,
                    RATIO_AVG_DEBIT_AMOUNT_AVG_CDT_AMT_30TO60D,
                    RATIO_TOTAL_DEBIT_AMOUNT_UPI_TOTAL_CREDIT_AMOUNT_UPI_90TO120D,
                    RATIO_NUM_CREDIT_TXNS_LESSTHAN_100_LAST30D_VS_150TO180D,
                    RATIO_TOTAL_DEBIT_AMOUNT_TOTAL_CREDIT_AMOUNT_60TO90D,
                    RATIO_TOTAL_CREDIT_AMOUNT_UPI_LAST30D_VS_150TO180D,
                    MAX_AVAILABLE_BALANCE_30TO60D,
                    RATIO_NUM_DEBIT_TXNS_500_TO_2000_NUM_CREDIT_TXNS_500_TO_2000_90TO120D,
                    TOTAL_CREDIT_COUNT_BANK_TRANSFER_30TO60D,
                    TOTAL_DEBIT_AMOUNT_UPI_LAST30D,
                    NUM_DEBIT_TXNS_LESSTHAN_100_LAST30D,
                    MIN_DEBIT_AMOUNT_90TO120D,
                    RATIO_NUM_CREDIT_TXNS_2000_TO_5000_LAST30D_VS_60TO90D,
                    RATIO_NUM_DEBIT_TXNS_LESSTHAN_100_NUM_CREDIT_TXNS_LESSTHAN_100_LAST30D,
                    RATIO_NUM_DEBIT_TXNS_GREATERTHAN_10000_LAST30D_VS_60TO90D,
                    RATIO_MAX_DEBIT_AMOUNT_LAST30D_VS_90TO120D,
                    RATIO_AVG_DEBIT_TXNS_AVG_CREDIT_TXNS_150TO180D,
                    MIN_CDT_AMT_150TO180D,
                    RATIO_AVG_AVAILABLE_BALANCE_LAST30D_VS_90TO120D,
                    NUM_CREDIT_TXNS_LESSTHAN_100_LAST30D,
                    NUM_CREDIT_TXNS_500_TO_2000_LAST30D,
                    NUM_CREDIT_TXNS_2000_TO_5000_LAST30D,
                    NUM_CREDIT_TXNS_5000_TO_10000_LAST30D,
                    NUM_CREDIT_TXNS_GREATERTHAN_10000_LAST30D,
                    RATIO_NUM_CREDIT_TXNS_GREATERTHAN_10000_LAST30D_VS_60TO90D,
                    IS_SMS_FEATURES_AVAILABLE,
                    NUM_MIN_BALANCE_BREACH_30D,
                    NUM_MIN_BALANCE_BREACH_180D,
                    NUM_READABLE_SMS_180D
                    from ANALYTICS.LENDING_PROD.LENDING_FACT
                    where date(disbursed_date)>=date('{sd}')
                    and date(disbursed_date)<=date('{ed}')
                    and IS_SMS_FEATURES_AVAILABLE = True
                """

            self.get_lending_fact_cols = """
                select 
                    a.CUSTOMER_ID as USER_ID,
                    a.DEVICE_ID,
                    a.BRE_RUN_DATE as DISBURSED_DATE,
                    a.NUM_CHEQUE_BOUNCES_30D,
                    a.NUM_CHEQUE_BOUNCES_60D,
                    a.NUM_CHEQUE_BOUNCES_90D,
                    a.NUM_CHEQUE_BOUNCES_120D,
                    a.NUM_CHEQUE_BOUNCES_150D,
                    a.NUM_CHEQUE_BOUNCES_180D,
                    a.NUM_NACH_BOUNCE_30D,
                    a.NUM_NACH_BOUNCE_60D,
                    a.NUM_NACH_BOUNCE_90D,
                    a.NUM_NACH_BOUNCE_120D,
                    a.NUM_NACH_BOUNCE_150D,
                    a.NUM_NACH_BOUNCE_180D,
                    a.NUM_LOAN_DEFAULT_30D,
                    a.NUM_LOAN_DEFAULT_60D,
                    a.NUM_LOAN_DEFAULT_90D,
                    a.NUM_LOAN_DEFAULT_120D,
                    a.NUM_LOAN_DEFAULT_150D,
                    a.NUM_LOAN_DEFAULT_180D,
                    a.NUM_CREDIT_CARD_OVERDUE_30D,
                    a.NUM_CREDIT_CARD_OVERDUE_60D,
                    a.NUM_CREDIT_CARD_OVERDUE_90D,
                    a.NUM_CREDIT_CARD_OVERDUE_120D,
                    a.NUM_CREDIT_CARD_OVERDUE_150D,
                    a.NUM_CREDIT_CARD_OVERDUE_180D,
                    a.NUM_MIN_BALANCE_BREACH_30D,
                    a.NUM_MIN_BALANCE_BREACH_60D,
                    a.NUM_MIN_BALANCE_BREACH_90D,
                    a.NUM_MIN_BALANCE_BREACH_120D,
                    a.NUM_MIN_BALANCE_BREACH_150D,
                    a.NUM_MIN_BALANCE_BREACH_180D,
                    a.IS_SMS_FEATURES_AVAILABLE,
                    b.NUM_READABLE_SMS_180D
                from ANALYTICS.LENDING_PROD.LENDING_FACT a 
                left join ANALYTICS.KB_ANALYTICS.USER_SMS_SUFF b 
                on trim(a.CUSTOMER_ID) = trim(b.USER_ID) and trim(a.BRE_RUN_ID) = trim(b.BRE_RUN_ID)
                where disbursed_date>=date('{sd}')
                and date(disbursed_date)<=date('{ed}')
                and IS_SMS_FEATURES_AVAILABLE = True
                """

        if self.dataset_name.strip().upper() == "COMBINATION_MODEL_XG":
            # SQL Query - getting BRE  data:
            self.get_raw_data = """
                select 
                    CUSTOMER_ID,
                    BRE_RUN_DATE,
                    BRE_RUN_ID,
                    OVERALL_BOUNCE_M0123,
                    AUTO_DEBIT_BOUNCE_M0,
                    AUTO_DEBIT_BOUNCE_M1,
                    BUREAUSCORE,
                    WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE,
                    WRITTEN_OFF_AMT_TOTAL_FOR_RULE,
                    SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE,
                    NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE,
                    SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE,
                    DEFAULT_ACCOUNTS_FOR_RULE,
                    MAX_DPD_2_YEARS_FOR_RULE,
                    PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    SCORE_CD_V2_EMSEMBLE_PROBABILITY,
                    NUM_READABLE_SMS_180D,
                    TOTAL_TRXNS,
                    DAYS_SINCE_LAST_TXN,
                    IS_SMS_FEATURES_AVAILABLE,
                    IS_LEDGER_FEATURES_AVAILABLE,
                    IS_BUREAU_FEATURES_AVAILBALE,
                    NUM_LOAN_OVERDUE_30D,
                    NUM_CREDIT_CARD_OVERDUE_30D,
                    NUM_LOAN_DEFAULT_30D,
                    NUM_CREDIT_CARD_DEFAULT_30D
                from ANALYTICS.LENDING_PROD.LENDING_FACT 
                where date(BRE_RUN_DATE) >=date('{sd}')
                and date(BRE_RUN_DATE)<=date('{ed}');
                """
            # SQL Query - getting txn module data:
            self.get_txn_data = """
                select * from ANALYTICS.KB_ANALYTICS.result_xgb_kb_txn_module_dag_version_two ; 
                """
            # SQL Query - getting activity module data:
            self.get_activity_data = """
                select * from ANALYTICS.KB_ANALYTICS.airflow_demo_write_result_xgb_kb_activity_module ; 
                """
            # SQL Query - getting sms module data
            self.get_sms_data = """
                select * from ANALYTICS.KB_ANALYTICS.result_xgb_kb_sms_module_dag_version_two ; 
                """
            # SQL Query - getting bureau module data
            self.get_bureau_data = """
                select * from ANALYTICS.KB_ANALYTICS.result_xgb_kb_bureau_module_dag_version_two ; 
                """

            # select a.*,b.Trx_PD,b.SMS_PD,b.Br_PD,b.CALIB_PD,b.logodds_trx, b.logodds_sms,b.logodds_br,b.COMBINED_LOGODDS
            # from ANALYTICS.LENDING_PROD.LENDING_FACT a
            # RIGHT join ANALYTICS.KB_ANALYTICS.FINAL_RESULT_COMBINATION_MODEL_XG_DAG_VERSION_TWO b
            # on trim(b.USER_ID) = trim(a.CUSTOMER_ID) and trim(b.BRE_RUN_ID) = trim(a.BRE_RUN_ID)
            # where disbursed_date>=date('{sd}')
            # and date(disbursed_date)<=date('{ed}')

            self.get_policy_run_gen_table = """
                select * from ANALYTICS.KB_ANALYTICS.FINAL_RESULT_RULE_ADD_COMBINATION_MODEL_XG_DAG_VERSION_TWO;
                """
            self.merge_master_table = """
                MERGE INTO ANALYTICS.KB_ANALYTICS.VERDICT_RESULT_MERGE_NEW_RULE_ADD J
                USING ANALYTICS.kb_analytics.VERDICT_RESULT_RULE_ADD_COMBINATION_MODEL_XG_DAG_VERSION_TWO C 
                ON trim(J.CUSTOMER_ID) = trim(C.CUSTOMER_ID) and trim(J.BRE_RUN_ID) = trim(C.BRE_RUN_ID) and trim(J.MODEL_TYPE) = trim(C.MODEL_TYPE)
                WHEN MATCHED THEN UPDATE SET 
                    J.CUSTOMER_ID = C.CUSTOMER_ID,
                    J.BRE_RUN_DATE = C.BRE_RUN_DATE,
                    J.BRE_RUN_ID = C.BRE_RUN_ID,
                    J.OVERALL_BOUNCE_M0123 = C.OVERALL_BOUNCE_M0123,
                    J.AUTO_DEBIT_BOUNCE_M0 = C.AUTO_DEBIT_BOUNCE_M0,
                    J.AUTO_DEBIT_BOUNCE_M1 = C.AUTO_DEBIT_BOUNCE_M1,
                    J.BUREAUSCORE = C.BUREAUSCORE,
                    J.WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE = C.WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE,
                    J.WRITTEN_OFF_AMT_TOTAL_FOR_RULE = C.WRITTEN_OFF_AMT_TOTAL_FOR_RULE,
                    J.SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE = C.SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE,
                    J.NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE = C.NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE,
                    J.SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE = C.SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE,
                    J.DEFAULT_ACCOUNTS_FOR_RULE = C.DEFAULT_ACCOUNTS_FOR_RULE,
                    J.MAX_DPD_2_YEARS_FOR_RULE = C.MAX_DPD_2_YEARS_FOR_RULE,
                    J.PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE = C.PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    J.SCORE_CD_V2_EMSEMBLE_PROBABILITY = C.SCORE_CD_V2_EMSEMBLE_PROBABILITY,
                    J.NUM_READABLE_SMS_180D = C.NUM_READABLE_SMS_180D,
                    J.TOTAL_TRXNS = C.TOTAL_TRXNS,
                    J.DAYS_SINCE_LAST_TXN = C.DAYS_SINCE_LAST_TXN,
                    J.IS_SMS_FEATURE = C.IS_SMS_FEATURE,
                    J.IS_BR_SUFF = C.IS_BR_SUFF,
                    J.IS_SMS_SUFF = C.IS_SMS_SUFF,
                    J.IS_TRX_SUFF = C.IS_TRX_SUFF,
                    J.COMBINATION_TYPE = C.COMBINATION_TYPE,
                    J.BR_PD = C.BR_PD,
                    J.LOGODDS_BR = C.LOGODDS_BR,
                    J.TRX_PD = C.TRX_PD,
                    J.LOGODDS_TRX = C.LOGODDS_TRX,
                    J.SMS_PD = C.SMS_PD,
                    J.LOGODDS_SMS = C.LOGODDS_SMS,
                    J.COMBINED_PD = C.COMBINED_PD,
                    J.COMBINED_LOGODDS = C.COMBINED_LOGODDS,
                    J.CALIB_PD = C.CALIB_PD,
                    J.RISK_BANDS = C.RISK_BANDS,
                    J.MODEL_TYPE = C.MODEL_TYPE,
                    J.NTC_FLAG = C.NTC_FLAG,
                    J.RULE2_OVERALL_BOUNCE_M0123_RULE_DECISION = C.RULE2_OVERALL_BOUNCE_M0123_RULE_DECISION,
                    J.RULE4_AUTO_DEBIT_BOUNCE_RULE_DECISION = C.RULE4_AUTO_DEBIT_BOUNCE_RULE_DECISION,
                    J.RULE5_WRITTEN_OFF_SETTLED_RULE_DECISION = C.RULE5_WRITTEN_OFF_SETTLED_RULE_DECISION,
                    J.RULE6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_DECISION = C.RULE6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_DECISION,
                    J.RULE7_NO_FOLL_SUIT_FILED_DEFAULT_RULE_DECISION = C.RULE7_NO_FOLL_SUIT_FILED_DEFAULT_RULE_DECISION,
                    J.RULE8_NO_FOLL_SUIT_FILED_WRITTEN_OFF_RULE_DECISION = C.RULE8_NO_FOLL_SUIT_FILED_WRITTEN_OFF_RULE_DECISION,
                    J.RULE9_MAX_DPD_2_YEARS_FOR_RULE = C.RULE9_MAX_DPD_2_YEARS_FOR_RULE,
                    J.RULE10_NO_FOLL_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE = C.RULE10_NO_FOLL_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    J.RULE11_NO_FOLL_STATUS_IN_24M_RULE = C.RULE11_NO_FOLL_STATUS_IN_24M_RULE,
                    J.RULE12_EXPERIAN_SCORE = C.RULE12_EXPERIAN_SCORE,
                    J.RULE13_NTC_FIS_PD_SCORE_RULE_DECISION = C.RULE13_NTC_FIS_PD_SCORE_RULE_DECISION,
                    J.RULE14_FIS_PD_SCORE_RULE_DECISION = C.RULE14_FIS_PD_SCORE_RULE_DECISION,
                    J.VERDICT_POLICY_ALONE = C.VERDICT_POLICY_ALONE,
                    J.VERDICT_POLICY_WITH_FB = C.VERDICT_POLICY_WITH_FB,
                    J.IS_SMS_FEATURES_AVAILABLE = C.IS_SMS_FEATURES_AVAILABLE,
                    J.IS_LEDGER_FEATURES_AVAILABLE = C.IS_LEDGER_FEATURES_AVAILABLE,
                    J.IS_BUREAU_FEATURES_AVAILBALE = C.IS_BUREAU_FEATURES_AVAILBALE,
                    J.NUM_LOAN_OVERDUE_30D = C.NUM_LOAN_OVERDUE_30D,
                    J.NUM_CREDIT_CARD_OVERDUE_30D = C.NUM_CREDIT_CARD_OVERDUE_30D,
                    J.NUM_LOAN_DEFAULT_30D = C.NUM_LOAN_DEFAULT_30D,
                    J.NUM_CREDIT_CARD_DEFAULT_30D = C.NUM_CREDIT_CARD_DEFAULT_30D

                WHEN NOT MATCHED THEN
                INSERT (
                    CUSTOMER_ID,
                    BRE_RUN_DATE,
                    BRE_RUN_ID,
                    OVERALL_BOUNCE_M0123,
                    AUTO_DEBIT_BOUNCE_M0,
                    AUTO_DEBIT_BOUNCE_M1,
                    BUREAUSCORE,
                    WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE,
                    WRITTEN_OFF_AMT_TOTAL_FOR_RULE,
                    SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE,
                    NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE,
                    SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE,
                    DEFAULT_ACCOUNTS_FOR_RULE,
                    MAX_DPD_2_YEARS_FOR_RULE,
                    PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    SCORE_CD_V2_EMSEMBLE_PROBABILITY,
                    NUM_READABLE_SMS_180D,
                    TOTAL_TRXNS,
                    DAYS_SINCE_LAST_TXN,
                    IS_SMS_FEATURE,
                    IS_BR_SUFF,
                    IS_SMS_SUFF,
                    IS_TRX_SUFF,
                    COMBINATION_TYPE,
                    BR_PD,
                    LOGODDS_BR,
                    TRX_PD,
                    LOGODDS_TRX,
                    SMS_PD,
                    LOGODDS_SMS,
                    COMBINED_PD,
                    COMBINED_LOGODDS,
                    CALIB_PD,
                    RISK_BANDS,
                    MODEL_TYPE,
                    NTC_FLAG,
                    RULE2_OVERALL_BOUNCE_M0123_RULE_DECISION,
                    RULE4_AUTO_DEBIT_BOUNCE_RULE_DECISION,
                    RULE5_WRITTEN_OFF_SETTLED_RULE_DECISION,
                    RULE6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_DECISION,
                    RULE7_NO_FOLL_SUIT_FILED_DEFAULT_RULE_DECISION,
                    RULE8_NO_FOLL_SUIT_FILED_WRITTEN_OFF_RULE_DECISION,
                    RULE9_MAX_DPD_2_YEARS_FOR_RULE,
                    RULE10_NO_FOLL_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    RULE11_NO_FOLL_STATUS_IN_24M_RULE,
                    RULE12_EXPERIAN_SCORE,
                    RULE13_NTC_FIS_PD_SCORE_RULE_DECISION,
                    RULE14_FIS_PD_SCORE_RULE_DECISION,
                    VERDICT_POLICY_ALONE,
                    VERDICT_POLICY_WITH_FB,
                    IS_SMS_FEATURES_AVAILABLE,
                    IS_LEDGER_FEATURES_AVAILABLE,
                    IS_BUREAU_FEATURES_AVAILBALE,
                    NUM_LOAN_OVERDUE_30D,
                    NUM_CREDIT_CARD_OVERDUE_30D,
                    NUM_LOAN_DEFAULT_30D,
                    NUM_CREDIT_CARD_DEFAULT_30D
                )
                VALUES (
                    C.CUSTOMER_ID,
                    C.BRE_RUN_DATE,
                    C.BRE_RUN_ID,
                    C.OVERALL_BOUNCE_M0123,
                    C.AUTO_DEBIT_BOUNCE_M0,
                    C.AUTO_DEBIT_BOUNCE_M1,
                    C.BUREAUSCORE,
                    C.WRITTEN_OFF_SETTLED_24MONTHS_CNT_FOR_RULE,
                    C.WRITTEN_OFF_AMT_TOTAL_FOR_RULE,
                    C.SUITFILED_WILFULDEFAULT_24MONTHS_FOR_RULE,
                    C.NEGATIVEACCOUNSTATUS_24MONTHS_FOR_RULE,
                    C.SUITFILEDWILLFULDEFAULTWRITTENOFFSTATUS_24MONTHS_FOR_RULE,
                    C.DEFAULT_ACCOUNTS_FOR_RULE,
                    C.MAX_DPD_2_YEARS_FOR_RULE,
                    C.PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    C.SCORE_CD_V2_EMSEMBLE_PROBABILITY,
                    C.NUM_READABLE_SMS_180D,
                    C.TOTAL_TRXNS,
                    C.DAYS_SINCE_LAST_TXN,
                    C.IS_SMS_FEATURE,
                    C.IS_BR_SUFF,
                    C.IS_SMS_SUFF,
                    C.IS_TRX_SUFF,
                    C.COMBINATION_TYPE,
                    C.BR_PD,
                    C.LOGODDS_BR,
                    C.TRX_PD,
                    C.LOGODDS_TRX,
                    C.SMS_PD,
                    C.LOGODDS_SMS,
                    C.COMBINED_PD,
                    C.COMBINED_LOGODDS,
                    C.CALIB_PD,
                    C.RISK_BANDS,
                    C.MODEL_TYPE,
                    C.NTC_FLAG,
                    C.RULE2_OVERALL_BOUNCE_M0123_RULE_DECISION,
                    C.RULE4_AUTO_DEBIT_BOUNCE_RULE_DECISION,
                    C.RULE5_WRITTEN_OFF_SETTLED_RULE_DECISION,
                    C.RULE6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_DECISION,
                    C.RULE7_NO_FOLL_SUIT_FILED_DEFAULT_RULE_DECISION,
                    C.RULE8_NO_FOLL_SUIT_FILED_WRITTEN_OFF_RULE_DECISION,
                    C.RULE9_MAX_DPD_2_YEARS_FOR_RULE,
                    C.RULE10_NO_FOLL_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    C.RULE11_NO_FOLL_STATUS_IN_24M_RULE,
                    C.RULE12_EXPERIAN_SCORE,
                    C.RULE13_NTC_FIS_PD_SCORE_RULE_DECISION,
                    C.RULE14_FIS_PD_SCORE_RULE_DECISION,
                    C.VERDICT_POLICY_ALONE,
                    C.VERDICT_POLICY_WITH_FB,
                    C.IS_SMS_FEATURES_AVAILABLE,
                    C.IS_LEDGER_FEATURES_AVAILABLE,
                    C.IS_BUREAU_FEATURES_AVAILBALE,
                    C.NUM_LOAN_OVERDUE_30D,
                    C.NUM_CREDIT_CARD_OVERDUE_30D,
                    C.NUM_LOAN_DEFAULT_30D,
                    C.NUM_CREDIT_CARD_DEFAULT_30D
                )
            """
        if self.dataset_name.strip().upper() == "MASTER_TABLE_GENERATION":
            self.get_bre_data = """
                select * from ANALYTICS.LENDING_PROD.LENDING_FACT 
                where 
            """

            self.merge_master_table = """
            Create or replace table ANALYTICS.KB_ANALYTICS.VERDICT_WITH_BAD_FLAG_NEW_RULE_ADD as
                select a.*, LOAN_ID,VENDOR, STATUS, pl.ever_15dpd_in_90days, pl.disbursed_date
                from ANALYTICS.KB_ANALYTICS.VERDICT_RESULT_MERGE_NEW_RULE_ADD a
                left join (select distinct *
                    from (
                    select USER_ID, LOAN_ID, DISBURSED_DATE,BRE_RUN_DATE,BRE_RUN_ID, VENDOR, STATUS,EVER_15DPD_IN_90DAYS,EVER_30DPD_IN_90DAYS,
                    datediff(day,LF.BRE_RUN_DATE,DISBURSED_DATE) as days_diff,
                    row_number () over (partition by USER_ID, LOAN_ID,DISBURSED_DATE order by days_diff asc) rank_CR
                    from ANALYTICS.KB_ANALYTICS.A0_UW_CUSTOMER_BASE_REPAYMENT_V1 pl
                    left join ANALYTICS.KB_ANALYTICS.VERDICT_RESULT_MERGE_NEW_RULE_ADD LF
                    on pl.user_id=LF.CUSTOMER_ID
                    and pl.disbursed_date>= LF.BRE_RUN_DATE
                    order by 1,2,3
                    )
                    where rank_CR=1
                    and DISBURSED_DATE>=date('{sd}')
                    and date(DISBURSED_DATE)<=date('{ed}')) pl
                on a.customer_id=pl.user_id
                and a.BRE_RUN_ID=pl.BRE_RUN_ID
                and a.BRE_RUN_DATE >='2022-08-01';
            """
            self.get_master_table = """ 
                select * from ANALYTICS.KB_ANALYTICS.VERDICT_WITH_BAD_FLAG_NEW_RULE_ADD
                where DISBURSED_DATE <  DATEADD(month, -3, GETDATE()) 
            """
            self.get_gini_table = """
                select 
                    a.*,
                    b.IS_SMS_FEATURE,
                    b.IS_BR_SUFF,
                    b.IS_SMS_SUFF,
                    b.IS_TRX_SUFF,
                    b.COMBINATION_TYPE,
                    b.BR_PD,
                    b.LOGODDS_BR,
                    b.TRX_PD,
                    b.LOGODDS_TRX,
                    b.SMS_PD,
                    b.LOGODDS_SMS,
                    b.COMBINED_PD,
                    b.COMBINED_LOGODDS,
                    b.CALIB_PD,
                    b.RISK_BANDS,
                    b.MODEL_TYPE,
                    b.NTC_FLAG,
                    b.RULE2_OVERALL_BOUNCE_M0123_RULE_DECISION,
                    b.RULE4_AUTO_DEBIT_BOUNCE_RULE_DECISION,
                    b.RULE5_WRITTEN_OFF_SETTLED_RULE_DECISION,
                    b.RULE6_WRITTEN_OFF_AMT_TOTAL_FOR_RULE_DECISION,
                    b.RULE7_NO_FOLL_SUIT_FILED_DEFAULT_RULE_DECISION,
                    b.RULE8_NO_FOLL_SUIT_FILED_WRITTEN_OFF_RULE_DECISION,
                    b.RULE9_MAX_DPD_2_YEARS_FOR_RULE,
                    b.RULE10_NO_FOLL_PAYMENTHISTORYPROFILE_12MONTHS_FOR_RULE,
                    b.RULE11_NO_FOLL_STATUS_IN_24M_RULE,
                    b.RULE12_EXPERIAN_SCORE,
                    b.RULE13_NTC_FIS_PD_SCORE_RULE_DECISION,
                    b.RULE14_FIS_PD_SCORE_RULE_DECISION,
                    b.VERDICT_POLICY_ALONE,
                    b.VERDICT_POLICY_WITH_FB,
                    b.LOAN_ID,
                    b.VENDOR,
                    b.STATUS,
                    b.EVER_15DPD_IN_90DAYS,
                    b.DISBURSED_DATE,
                    b.MONTH_YEAR,
                    b.CALIB_PD_GINI_OVERALL,
                    b.CALIB_PD_GINI,
                    b.CALIB_PD_AND_FB_SCORE_GINI,
                    b.SMS_PD_GINI_OVERALL,
                    b.SMS_PD_GINI,
                    b.SMS_PD_AND_FB_SCORE_GINI,
                    b.TRX_PD_GINI_OVERALL,
                    b.TRX_PD_GINI,
                    b.TRX_PD_AND_FB_SCORE_GINI,
                    b.BR_PD_GINI_OVERALL,
                    b.BR_PD_GINI,
                    b.BR_PD_AND_FB_SCORE_GINI,
                    b.SCORE_CD_V2_EMSEMBLE_PROBABILITY_GINI_OVERALL,
                    b.SCORE_CD_V2_EMSEMBLE_PROBABILITY_GINI
                from ANALYTICS.LENDING_PROD.LENDING_FACT a
                right join ANALYTICS.KB_ANALYTICS.VERDICT_WITH_GINI_PSI_RULE_ADD_MASTER_TABLE_GENERATION_DAG_VERSION_TWO b 
                on trim(a.CUSTOMER_ID) = trim(b.CUSTOMER_ID) and trim(a.BRE_RUN_ID) = trim(b.BRE_RUN_ID); 

            """
