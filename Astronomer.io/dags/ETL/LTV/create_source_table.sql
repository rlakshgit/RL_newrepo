CREATE TABLE if not exists `{project}.{dataset}.{table}`
                           (
                                AccountNumber   STRING,
                                PolicyNumber   STRING,
                                JobNumber    STRING,
                                TermNumber      STRING,
                                ModelNumber       int64,
                                PeriodEffDate      int64,
                                TransEffDate   int64,
                                JobCloseDate    int64,
                                AccountingDate   int64,
                                PolicyYear   int64,
                                PolicyMonth   int64,
                                SourceOfBusiness    STRING,
                                InsuredAge   int64,
                                InsuredGender     STRING,
                                InsuredState   STRING,
                                InsuredPostalCode   STRING,
                                InsuredCountry   STRING,
                                ItemNumber    int64,
                                RiskType   STRING,
                                CoverageTypeCode   STRING,
                                IsInactive   int64,
                                ItemClass    STRING,
                                ItemLimit    NUMERIC,
                                RiskState   STRING,
                                RiskCountry    STRING,
                                RiskPostalCode   STRING,
                                WearerAge   int64,
                                ItemRank   int64,
                                bq_load_date DATE					  
                            )
                            PARTITION BY
                            bq_load_date;