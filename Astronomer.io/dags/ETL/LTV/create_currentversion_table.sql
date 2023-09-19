CREATE TABLE if not exists `{project}.{dataset}.{table}`
                            (
                                AccountNumber STRING,
                                PolicyNumber STRING,
                                LTV FLOAT64,
                                LTV_model_version FLOAT64,
                                PolicyProfile FLOAT64,
                                PP_model_version FLOAT64,
                                currentversion_load_date DATE	
                                )PARTITION BY
                                currentversion_load_date;