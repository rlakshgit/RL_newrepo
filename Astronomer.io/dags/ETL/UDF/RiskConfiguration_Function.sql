CREATE OR REPLACE PROCEDURE `{dest_dataset}.build_config_risk`(input_table STRING, output_table STRING)
BEGIN

SET input_table = `input_table`;
EXECUTE IMMEDIATE format("""

CREATE OR REPLACE TEMP TABLE dt_tran AS 
(
  SELECT 
    PolicyNumber,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    WrittenDate,
    CancelEffDate,
    CloseBeg,
    ProductType,
    RiskNumber,
    IsInactive,
    ActiveFlag, 
    CASE 
      WHEN (TranType = 'Cancellation' OR (TranType = 'Policy Change' AND IsInactive = 1)) AND TransEffDate = PeriodEffDate THEN 1 
      WHEN (TranType = 'Cancellation' OR (TranType = 'Policy Change' AND IsInactive = 1)) AND 
          TransEffDate <= MIN(CASE WHEN IsInactive = 0 AND TranType != 'Cancellation' THEN TransEffDate END) 
                                OVER(PARTITION BY PolicyNumber,
                                                  TermNumber,
                                                  RiskNumber
                                                  -- CUMSUM
                                        ORDER BY	ModelNumber
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
          THEN 1 
      ELSE 0 
    END AS TermNFlag
    FROM %s
    );
    """, input_table);

  -- FROM `{dest_dataset}.k_cl_bop_config_01` AS bop_tran ); --need to be parameter 

CREATE OR REPLACE TEMP TABLE dt_exp AS 
(
  WITH reinstNewTerm AS 
  (
    SELECT
      r.PolicyNumber,
      r.TermNumber,
      r.ModelNumber,
      CAST(TermNumber + 
        SUM(AddNew) 
          OVER (PARTITION BY  r.PolicyNumber,
                              r.TermNumber
                ORDER BY      r.ModelNumber
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS STRING) AS TNModified
    FROM
    (
      SELECT DISTINCT
        PolicyNumber,
        TermNumber,
        ModelNumber,
        CASE WHEN TranType = 'Reinstatement' THEN 0.0001 ELSE 0.0000 END AS AddNew
      FROM dt_tran
    )r
  )
  ,ActiveCount AS 
  (
    SELECT 
      dt_tran.PolicyNumber,
      dt_tran.TermNumber,
      dt_tran.RiskNumber,
      SUM(CASE WHEN IsInactive = 0 AND TranType != 'Cancellation' THEN 1 ELSE 0 END) AS TermActiveCount -- IM/BOP didn't have any transactions with zero count
    FROM dt_tran
    GROUP BY
      dt_tran.PolicyNumber,
      dt_tran.TermNumber,
      dt_tran.RiskNumber
  )
  SELECT
    dt_tran.PolicyNumber,
    dt_tran.ProductType,
    dt_tran.IsInactive,
    dt_tran.PeriodEffDate,
    dt_tran.PeriodEndDate,
    dt_tran.JobNumber,
    dt_tran.TranType,
    dt_tran.TermNumber,
    dt_tran.ModelNumber,
    dt_tran.TransEffDate,
    dt_tran.JobCloseDate,
    dt_tran.CancelEffDate,
    reinstNewTerm.TNModified,
    dt_tran.RiskNumber,
    dt_tran.CloseBeg,
    ActiveCount.TermActiveCount,
    sum(ActiveFlag)      OVER (PARTITION BY dt_tran.PolicyNumber,
                                            dt_tran.TermNumber,
                                            dt_tran.RiskNumber
                                ORDER BY    dt_tran.ModelNumber
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS ActiveCumSum,
    sum(IsInactive)      OVER (PARTITION BY dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY    dt_tran.ModelNumber
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)       AS InactiveCumSum,
    sum(TermNFlag)      OVER (PARTITION BY dt_tran.PolicyNumber,
                                            dt_tran.TermNumber,
                                            dt_tran.RiskNumber
                                ORDER BY    dt_tran.ModelNumber
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)       AS TermNFlagCumSum,
    LAG(TranType,1)      OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS prevTranType,
    LAG(TransEffDate,1)  OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS prevTransEff,
    LEAD(TranType,1)     OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS nextTranType,
    LEAD(TransEffDate,1) OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS nextTransEff,
    LEAD(CloseBeg,1)     OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS nextCloseBeg,
    LEAD(IsInactive,1)   OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS nextIsInactive,
    LAG(IsInactive,1)    OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS prevIsInactive,
    LAG(CancelEffDate,1) OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)					  AS prevCancelEff,
    LAG(PeriodEndDate,1) OVER (PARTITION BY	dt_tran.PolicyNumber,
                                            reinstNewTerm.TNModified,
                                            dt_tran.RiskNumber
                                ORDER BY	dt_tran.ModelNumber)						AS prevPeriodEnd
  FROM dt_tran
    LEFT JOIN reinstNewTerm 
    ON reinstNewTerm.PolicyNumber = dt_tran.PolicyNumber
    AND reinstNewTerm.TermNumber = dt_tran.TermNumber 
    AND reinstNewTerm.ModelNumber = dt_tran.ModelNumber

    LEFT JOIN ActiveCount
    ON ActiveCount.PolicyNumber = dt_tran.PolicyNumber
    AND ActiveCount.TermNumber = dt_tran.TermNumber 
    AND ActiveCount.RiskNumber = dt_tran.RiskNumber
);
#####*************** Set Prior inactive effective date for item to change tran eff ***************#####
CREATE OR REPLACE TEMP TABLE dt_exp01 AS 
(
  WITH dt_modeleff AS 
  (
    SELECT DISTINCT
      PolicyNumber,
      TNModified,
      ModelNumber AS IM,
      TransEffDate AS InactiveChangeEff
    FROM dt_exp
  )
  ,dt_inactive AS 
  (
    SELECT 
      *,        
      MAX(InactiveEff)      
          OVER (PARTITION BY  i.PolicyNumber,
                              i.TNModified,
                              i.RiskNumber
                  ORDER BY    i.ModelNumber
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS IM
    FROM 
    (
      SELECT 
        PolicyNumber,
        TNModified,
        RiskNumber,
        ModelNumber,
        IsInactive,
        prevIsInactive,
        TransEffDate,
        JobCloseDate,
        TranType,
        IF(IsInactive = 1 AND TranType IN ('Policy Change','Issuance') AND prevIsInactive = 0,ModelNumber,0) AS InactiveEff
      FROM dt_exp
    )i
  )
  ,dt_inactive1 AS 
  (
    SELECT 
      PolicyNumber,
      TNModified,
      ModelNumber,
      RiskNumber,
      InactiveChangeEff
    FROM dt_inactive
      INNER JOIN dt_modeleff USING (PolicyNumber,TNModified,IM)
    WHERE prevIsInactive = 1 AND IsInactive = 0 AND TranType != 'Cancellation' AND InactiveChangeEff > TransEffDate AND JobCloseDate > InactiveChangeEff 
  --IM: PolicyNumber = '55-008578' AND RiskNumber = 1 AND TermNumber = 9
  --BOP: PolicyNumber = '55-001926' AND RiskNumber = '5_1' AND TermNumber = 10
  )
  SELECT 
    dt_exp.PolicyNumber,
    dt_exp.ProductType,
    dt_exp.IsInactive,
    dt_exp.PeriodEffDate,
    dt_exp.PeriodEndDate,
    dt_exp.JobNumber,
    dt_exp.TranType,
    dt_exp.TermNumber,
    dt_exp.ModelNumber,
    dt_exp.TransEffDate,
    dt_exp.JobCloseDate,
    dt_exp.CancelEffDate,
    dt_exp.TNModified,
    dt_exp.RiskNumber,
    dt_exp.CloseBeg,
    dt_exp.TermActiveCount,
    dt_exp.ActiveCumSum,
    CASE WHEN prevIsInactive IS NULL THEN COALESCE(dt_exp.InactiveCumSum,dt_exp.IsInactive) ELSE dt_exp.InactiveCumSum END AS InactiveCumSum,
    MIN(CASE WHEN IsInactive = 0 AND TranType != 'Cancellation' THEN TransEffDate END) 
        OVER(PARTITION BY dt_exp.PolicyNumber,
                          dt_exp.TNModified,
                          dt_exp.RiskNumber,
                          COALESCE(dt_exp.TermNFlagCumSum,0)
                ORDER BY	dt_exp.ModelNumber
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)                          AS MinActiveEffTermN,
    dt_exp.prevTranType,
    dt_exp.prevTransEff,
    dt_exp.nextTranType,
    dt_exp.nextTransEff,
    dt_exp.nextCloseBeg,
    dt_exp.nextIsInactive,
    dt_exp.prevIsInactive,
    dt_exp.prevCancelEff,
    dt_inactive1.InactiveChangeEff,
    COALESCE(dt_inactive1.InactiveChangeEff,TransEFfDate) AS TranBeg,
    CASE    
      WHEN prevTranType IS NULL OR (prevIsInactive = 1 AND IsInactive = 0) THEN COALESCE(dt_inactive1.InactiveChangeEff,TransEFfDate)
      WHEN TranType = 'Issuance' AND prevPeriodEnd != PeriodEndDate AND CloseBeg > prevPeriodEnd THEN IF(TransEffDate < prevPeriodEnd,PeriodEffDate,TransEffDate) --IM: N/A; BOP: N/A
      ELSE CloseBeg
    END AS ExpBegCY,
    COALESCE(nextCloseBeg,PeriodEndDate) AS ExpEndCY,
    CASE
      WHEN prevTranType IS NULL OR (prevIsInactive = 1 AND IsInactive = 0) THEN COALESCE(dt_inactive1.InactiveChangeEff,TransEFfDate)
      --IM: PolicyNumber = '55-001019' AND RiskNumber = 1 AND TermNumber = 2
      --BOP: PolicyNumber = '55-015246' AND RiskNumber = '8_1' AND TermNumber = 3; PolicyNumber = '55-016385' AND RiskNumber = '2_1' AND TermNumber = 2
      WHEN TranType = 'Issuance' AND prevPeriodEnd != PeriodEndDate THEN IF(prevPeriodEnd < TransEffDate,TransEffDate,prevPeriodEnd) --IM: N/A; BOP: N/A
      ELSE TransEffDate
    END AS WeBegCY,
    CASE
      WHEN prevTranType IS NULL OR (prevIsInactive = 1 AND IsInactive = 0) THEN PeriodEndDate
      WHEN TranType = 'Issuance' AND prevPeriodEnd != PeriodEndDate THEN PeriodEndDate
      ELSE TransEFfDate
    END AS WeEndCY
  FROM dt_exp
    LEFT JOIN dt_inactive1 USING (PolicyNumber,TNModified,ModelNumber,RiskNumber)     
);
CREATE OR REPLACE TEMP TABLE dt_exp01_a AS 
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    CASE 
      WHEN prevIsInactive = 1 AND IsInactive = 1 
        THEN LAST_VALUE(MinActiveEffTermN IGNORE NULLS) OVER(PARTITION BY PolicyNumber,TermNumber,RiskNumber 
                                                              ORDER BY ModelNumber ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ELSE MinActiveEffTermN 
    END AS MinActiveEffTermN, --PolicyNumber = '55-006228' AND RiskNumber = 5 AND TermNumber = 2 For multi inactive cases
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    prevCancelEff,
    TranBeg,
    CASE  
      WHEN TranType = 'Cancellation' AND prevIsInactive IS NULL THEN TransEFfDate 
      --IM: PolicyNumber = '55-007314' AND RiskNumber = 12 AND TermNumber = 2
      --BOP: PolicyNumber = '55-007314' AND RiskNumber = '12_1' AND TermNumber = 2
      WHEN TranType = 'Cancellation' OR (TranType IN ('Policy Change','Issuance') AND prevIsInactive = 0 AND IsInactive = 1) -- Inactive or Cancel 
        THEN IF(TranBeg < MinActiveEffTermN AND TransEffDate < JobCloseDate,MinActiveEffTermN,TranBeg) 
        --IM: PolicyNumber = '55-010579' AND RiskNumber = 1 AND TermNumber = 6;PolicyNumber = '55-015410' AND RiskNumber = 7 AND TermNumber = 2;PolicyNumber = '55-008984' AND RiskNumber = 1 AND TermNumber = 12;
        --BOP: PolicyNumber = '55-000375' AND RiskNumber = '1_1' AND TermNumber = 11;PolicyNumber = '55-003355' AND RiskNumber = '2_1' AND TermNumber = 10;PolicyNumber = '55-000006' AND RiskNumber = '1_1' AND TermNumber = 10;
      ELSE ExpBegCY
    END AS ExpBegCY,
    CASE    
      WHEN TranType = 'Cancellation' AND prevIsInactive IS NULL THEN TransEFfDate
      WHEN TranType = 'Cancellation' OR (TranType IN ('Policy Change','Issuance') AND prevIsInactive = 0 AND IsInactive = 1) -- Inactive or Cancel 
        THEN IF(TransEffDate >= JobCloseDate,TranBeg,IF(MinActiveEffTermN > CloseBeg,MinActiveEffTermN,CloseBeg)) --BOP: PolicyNumber = '55-000342' AND RiskNumber = '10_1' AND TermNumber = 11;
      ELSE ExpEndCY
    END AS ExpEndCY,
    CASE  
      WHEN TranType = 'Cancellation' AND prevIsInactive IS NULL THEN TransEFfDate
      WHEN TranType = 'Cancellation' OR (TranType IN ('Policy Change','Issuance') AND prevIsInactive = 0 AND IsInactive = 1) -- Inactive or Cancel 
        THEN IF(TransEFfDate < MinActiveEffTermN,MinActiveEffTermN,TransEffDate)
      ELSE WeBegCY
    END AS WeBegCY,
    CASE    
      WHEN TranType = 'Cancellation' AND prevIsInactive IS NULL THEN TransEFfDate
      WHEN TranType = 'Cancellation' OR (TranType IN ('Policy Change','Issuance') AND prevIsInactive = 0 AND IsInactive = 1) -- Inactive or Cancel 
        THEN PeriodEndDate
      ELSE WeEndCY
    END AS WeEndCY
  FROM dt_exp01
);
CREATE OR REPLACE TEMP TABLE dt_exp01_b AS  -- Multi Cancel
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    MinActiveEffTermN,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,
    ExpBegCY,
    CASE    
      WHEN prevTranType = 'Cancellation' AND TranType = 'Cancellation' AND IsInactive = 0 
      --IM: PolicyNumber = '55-015757' AND RiskNumber = 1 AND TermNumber = 1
      --BOP: PolicyNumber = '55-002465' AND RiskNumber = '1_1' AND TermNumber = 7
        THEN IF(TransEffDate > prevTransEff,ExpBegCY,prevTransEff)
      WHEN prevTranType = 'Policy Change' AND TranType = 'Cancellation' AND prevCancelEff IS NOT NULL --IM: N/A; BOP: N/A
        THEN IF(TransEffDate > prevCancelEff,ExpBegCY,prevCancelEff)
      ELSE ExpEndCY
    END AS ExpEndCY,
    WeBegCY,
    CASE    
      WHEN prevTranType = 'Cancellation' AND TranType = 'Cancellation' AND IsInactive = 0
        THEN IF(TransEffDate > prevTransEff,WeBegCY,prevTransEff)
      WHEN prevTranType = 'Policy Change' AND TranType = 'Cancellation' AND prevCancelEff IS NOT NULL
        THEN IF(TransEffDate > prevCancelEff,WeBegCY,prevCancelEff)
      ELSE WeEndCY
    END AS WeEndCY,
    CASE    
      WHEN TranType IN ('Policy Change','Issuance') AND CancelEffDate IS NOT NULL THEN 
        IF(TransEffDate > CancelEffDate OR
                (prevIsInactive = 0 AND IsInactive = 0 AND prevIsInactive IS NOT NULL) OR
                (prevIsInactive = 1 AND IsInactive = 1 AND prevIsInactive IS NOT NULL)
            ,1
            ,2)
      ELSE 0
    END AS ChangeAfterCan
  FROM dt_exp01_a
);
CREATE OR REPLACE TEMP TABLE dt_exp01_c AS  -- Policy Change After Cancel
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    MinActiveEffTermN,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,ChangeAfterCan,
    IF(ChangeAfterCan = 0,ExpBegCY,TranBeg) AS ExpBegCY,
    CASE    
      WHEN ChangeAfterCan = 1 THEN TranBeg
      --IM: PolicyNumber = '55-016516' AND RiskNumber = 1 AND TermNumber = 2
      --BOP: PolicyNumber = '55-015626' AND RiskNumber = '1_1' AND TermNumber = 1
      WHEN ChangeAfterCan = 2 THEN CancelEffDate --IM: N/A; BOP: N/A
      ELSE ExpEndCY
    END AS ExpEndCY,
    CASE
      WHEN ChangeAfterCan = 2 AND prevIsInactive IS NULL AND IsInactive = 0 THEN TransEffDate
      WHEN ChangeAfterCan = 2 AND prevIsInactive = 1 AND IsInactive = 0 THEN TranBeg
      WHEN ChangeAfterCan = 2 AND prevIsInactive = 0 AND IsInactive = 1 THEN TranBeg
      WHEN ChangeAfterCan = 1 THEN TransEffDate
      ELSE WeBegCY
    END WeBegCY,
    CASE
      WHEN ChangeAfterCan = 2 AND prevIsInactive IS NULL AND IsInactive = 0 THEN CancelEffDate
      WHEN ChangeAfterCan = 2 AND prevIsInactive = 1 AND IsInactive = 0 THEN CancelEffDate
      WHEN ChangeAfterCan = 2 AND prevIsInactive = 0 AND IsInactive = 1 THEN CancelEffDate
      WHEN ChangeAfterCan = 1 THEN TransEffDate
      ELSE WeEndCY
    END WeEndCY
  FROM dt_exp01_b
);
CREATE OR REPLACE TEMP TABLE dt_exp01_d AS  -- Policy Change After Expiration
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    MinActiveEffTermN,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,
    ExpBegCY,
    CASE    
      --IM: PolicyNumber = '55-010421' AND RiskNumber = 1 AND TermNumber = 8;PolicyNumber = '55-012280' AND RiskNumber = 1 AND TermNumber = 6; PolicyNumber = '55-014012' AND RiskNumber = 2 AND TermNumber = 4
      --BOP: PolicyNumber = '55-016510' AND RiskNumber = '1_1' AND TermNumber = 1;PolicyNumber = '55-002281' AND RiskNumber = '1_1' AND TermNumber = 11
      WHEN TranType IN ('Policy Change','Issuance') AND JobCloseDate > PeriodEndDate AND prevIsInactive =  0 AND IsInactive = 0 AND prevIsInactive IS NOT NULL THEN ExpBegCY 
      ELSE ExpEndCY
    END AS ExpEndCY,
    WeBegCY,
    WeEndCY

  FROM dt_exp01_c
);
CREATE OR REPLACE TEMP TABLE dt_exp01_e AS  -- Policy Change with next tran closed before tran eff date 
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    MinActiveEffTermN,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,
    ExpBegCY,
    CASE    
      --IM: PolicyNumber = '55-000342' AND RiskNumber = 1 AND TermNumber = 11
      --BOP: PolicyNumber = '55-001978' AND RiskNumber = '1_1' AND TermNumber = 10;
      WHEN TranType IN ('Policy Change','Issuance') AND nextCloseBeg < ExpBegCY AND nextCloseBeg IS NOT NULL THEN IF(ExpBegCY >= nextTransEff,ExpBegCY,nextTransEff)
      ELSE ExpEndCY
    END AS ExpEndCY,
    MIN(CASE WHEN IsInactive = 1 OR TranType = 'Cancellation' THEN TransEffDate END) 
      OVER(PARTITION BY   PolicyNumber,
                          TermNumber,
                          RiskNumber,
                          ActiveCumSum
            ORDER BY      ModelNumber
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)               AS PriorGrpMinInactiveEff,
    WeBegCY,
    WeEndCY
  FROM dt_exp01_d
);
CREATE OR REPLACE TEMP TABLE dt_exp01_f AS  --End to be same as Beg for Inactive items without prior or with prior inactive 
(
  WITH dt_minIClose AS --IM: PolicyNumber = '55-005262' AND RiskNumber = 1 AND TermNumber = 1; BOP: PolicyNumber = '55-013694' AND RiskNumber = '4_1' AND TermNumber = 6
  (
    SELECT 
      PolicyNumber,
      TermNumber,
      RiskNumber,
      TransEffDate AS PriorGrpMinInactiveEff,
      MIN(JobCloseDate) AS MinClose
    FROM dt_exp01_e
    WHERE IsInactive = 1
    GROUP BY
      PolicyNumber,
      TermNumber,
      RiskNumber,
      TransEffDate
  )
  SELECT 
    dt_exp01_e.PolicyNumber,
    dt_exp01_e.ProductType,
    dt_exp01_e.IsInactive,
    dt_exp01_e.PeriodEffDate,
    dt_exp01_e.PeriodEndDate,
    dt_exp01_e.JobNumber,
    dt_exp01_e.TranType,
    dt_exp01_e.TermNumber,
    dt_exp01_e.ModelNumber,
    dt_exp01_e.TransEffDate,
    dt_exp01_e.JobCloseDate,
    dt_exp01_e.CancelEffDate,
    dt_exp01_e.TNModified,
    dt_exp01_e.RiskNumber,
    dt_exp01_e.CloseBeg,
    dt_exp01_e.TermActiveCount,
    dt_exp01_e.ActiveCumSum,
    dt_exp01_e.InactiveCumSum,
    dt_exp01_e.MinActiveEffTermN,
    dt_exp01_e.prevTranType,
    dt_exp01_e.prevTransEff,
    dt_exp01_e.nextTranType,
    dt_exp01_e.nextTransEff,
    dt_exp01_e.nextCloseBeg,
    dt_exp01_e.nextIsInactive,
    dt_exp01_e.prevIsInactive,
    dt_exp01_e.TranBeg,
    CASE 
      WHEN dt_exp01_e.IsInactive = 1 AND dt_exp01_e.prevIsInactive = 1 AND dt_exp01_e.TransEFfDate < dt_exp01_e.PriorGrpMinInactiveEff AND dt_exp01_e.ActiveCumSum > 0 
        THEN IF(dt_exp01_e.TransEFfDate <= dt_exp01_e.MinActiveEffTermN --IM: PolicyNumber = '55-009256' AND RiskNumber = 2 AND TermNumber = 8; BOP: PolicyNumber = '55-008561' AND RiskNumber = '25_1' AND TermNumber = 9
                ,MinActiveEffTermN
                ,dt_exp01_e.TransEFfDate) --IM: PolicyNumber = '55-008064' AND RiskNumber = 2 AND TermNumber = 9; BOP: PolicyNumber = '55-001810' AND RiskNumber = '1_1' AND TermNumber = 11
      ELSE dt_exp01_e.ExpBegCY 
    END AS ExpBegCY,
    CASE    
      WHEN dt_exp01_e.IsInactive IS NULL OR dt_exp01_e.TermActiveCount = 0 THEN dt_exp01_e.ExpBegCY --IM: N/A; BOP: N/A
      WHEN dt_exp01_e.prevIsInactive IS NULL AND dt_exp01_e.IsInactive = 1 THEN dt_exp01_e.ExpBegCY 
      --IM: PolicyNumber = '55-002718' AND RiskNumber = 1 AND TermNumber = 11;
      --BOP: PolicyNumber = '55-011808' AND RiskNumber = '1_1' AND TermNumber = 7; PolicyNumber = '55-004665' AND RiskNumber = '1_1' AND TermNumber = 10;
      WHEN dt_exp01_e.prevIsInactive = 1 AND dt_exp01_e.IsInactive = 1 
        THEN IF(dt_exp01_e.TransEFfDate < dt_exp01_e.PriorGrpMinInactiveEff AND dt_exp01_e.ActiveCumSum > 0
                -- ,IF(dt_exp01_e.JobCloseDate = dt_minIClose.MinClose AND dt_exp01_e.prevTransEff > dt_exp01_e.JobCloseDate,dt_exp01_e.CloseBeg,dt_exp01_e.PriorGrpMinInactiveEff)
                ,CASE 
                  WHEN dt_exp01_e.PriorGrpMinInactiveEff <= dt_exp01_e.MinActiveEffTermN THEN dt_exp01_e.MinActiveEffTermN 
                  --IM: PolicyNumber = '55-006228' AND RiskNumber = 5 AND TermNumber = 2
                  --BOP: PolicyNumber = '55-004930' AND RiskNumber = '11_1' AND TermNumber = 10
                  WHEN dt_exp01_e.JobCloseDate = dt_minIClose.MinClose AND dt_exp01_e.prevTransEff > dt_exp01_e.JobCloseDate THEN dt_exp01_e.CloseBeg --IM: N/A; BOP: N/A
                  WHEN dt_exp01_e.PriorGrpMinInactiveEff > dt_exp01_e.JobCloseDate THEN dt_exp01_e.CloseBeg
                  --IM: PolicyNumber = '55-008064' AND RiskNumber = 2 AND TermNumber = 9;PolicyNumber = '55-006676' AND RiskNumber = 1 AND TermNumber = 9
                  --BOP: PolicyNumber = '55-001978' AND RiskNumber = '1_1' AND TermNumber = 10;PolicyNumber = '55-000159' AND RiskNumber = '2_1' AND TermNumber = 11
                  ELSE dt_exp01_e.PriorGrpMinInactiveEff --BOP: PolicyNumber = '55-000469' AND RiskNumber = '2_1' AND TermNumber = 11
                  END
                ,dt_exp01_e.ExpBegCY) --BOP: PolicyNumber = '55-001978' AND RiskNumber = '1_1' AND TermNumber = 10; PolicyNumber = '55-003881' AND RiskNumber = '1_1' AND TermNumber = 11
      ELSE dt_exp01_e.ExpEndCY
    END AS ExpEndCY,
    CASE 
      WHEN dt_exp01_e.IsInactive = 1 AND dt_exp01_e.prevIsInactive = 1 AND dt_exp01_e.TransEFfDate < dt_exp01_e.PriorGrpMinInactiveEff  AND dt_exp01_e.ActiveCumSum > 0 
        THEN IF(dt_exp01_e.TransEFfDate <= dt_exp01_e.MinActiveEffTermN,MinActiveEffTermN,dt_exp01_e.TransEFfDate)
      ELSE WeBegCY 
    END AS WeBegCY,
    CASE    
      WHEN dt_exp01_e.IsInactive IS NULL OR dt_exp01_e.TermActiveCount = 0 THEN dt_exp01_e.WeBegCY
      WHEN dt_exp01_e.prevIsInactive IS NULL AND dt_exp01_e.IsInactive = 1 THEN dt_exp01_e.WeBegCY
      WHEN dt_exp01_e.prevIsInactive = 1 AND dt_exp01_e.IsInactive = 1 
        THEN IF(dt_exp01_e.TransEFfDate < dt_exp01_e.PriorGrpMinInactiveEff AND dt_exp01_e.ActiveCumSum > 0
                ,IF(dt_exp01_e.PriorGrpMinInactiveEff <= dt_exp01_e.MinActiveEffTermN,dt_exp01_e.MinActiveEffTermN,dt_exp01_e.PriorGrpMinInactiveEff)
                ,dt_exp01_e.WeBegCY) 
      ELSE dt_exp01_e.WeEndCY
    END AS WeEndCY
  FROM dt_exp01_e
    LEFT JOIN dt_minIClose USING(PolicyNumber,TermNumber,RiskNumber,PriorGrpMinInactiveEff)
);
CREATE OR REPLACE TEMP TABLE dt_exp02 AS --Cancellation with IsInactive = 0 with prevIsInactive = 1 AND JobClose after Inactive
(
  WITH dt_min_inactive AS 
  (
    SELECT 
      PolicyNumber,
      TermNumber,
      RiskNumber,
      ActiveCumSum + 1 AS ActiveCumSum,
      MIN(CASE WHEN IsInactive = 1 THEN ExpBegCY END) AS MinInactiveEff
    FROM dt_exp01_f
    GROUP BY    
      PolicyNumber,
      TermNumber,
      RiskNumber,
      ActiveCumSum + 1
  )
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    MinActiveEffTermN,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,
    CASE    
      WHEN TranType = 'Cancellation' AND prevIsInactive = 1 AND IsInactive = 0 AND ExpBegCY != ExpEndCY AND ExpBegCY > MinInactiveEff THEN MinInactiveEff  
      --BOP: PolicyNumber = '55-005755' AND RiskNumber = '3_1' AND TermNumber = 7
      ELSE ExpBegCY
    END AS ExpBegCY,
    CASE    
      WHEN TranType = 'Cancellation' AND prevIsInactive = 1 AND IsInactive = 0 AND ExpBegCY != ExpEndCY AND ExpEndCY > MinInactiveEff THEN MinInactiveEff  
      --IM: PolicyNumber = '55-011359' AND RiskNumber = 1 AND TermNumber = 5;PolicyNumber = '55-007688' AND RiskNumber = 2 AND TermNumber = 8
      --BOP: PolicyNumber = '55-000574' AND RiskNumber = '2_1' AND TermNumber = 7;PolicyNumber = '55-012177' AND RiskNumber = '2_2' AND TermNumber = 6
      ELSE ExpEndCY
    END AS ExpEndCY,
    CASE  
      WHEN TranType = 'Cancellation' AND prevIsInactive = 1 AND IsInactive = 0 AND WeBegCY != WeEndCY AND WeBegCY > MinInactiveEff THEN MinInactiveEff 
      ELSE WeBegCY
    END AS WeBegCY,
    CASE  
      WHEN TranType = 'Cancellation' AND prevIsInactive = 1 AND IsInactive = 0 AND WeBegCY != WeEndCY AND WeEndCY > MinInactiveEff THEN MinInactiveEff 
      ELSE WeEndCY
    END AS WeEndCY,
    RANK() OVER(PARTITION BY PolicyNumber,TermNumber,RiskNumber ORDER BY CloseBeg ASC,ModelNumber ASC) AS CloseBegRank
  FROM dt_exp01_f
    LEFT JOIN dt_min_inactive USING (PolicyNumber,TermNumber,RiskNumber,ActiveCumSum)
);
CREATE OR REPLACE TEMP TABLE dt_exp03 AS  -- Out of sequence
(
  WITH dt_oos AS 
  (
    SELECT 
      PolicyNumber,
      RiskNumber,
      TermNumber,
      ModelNumber,
      TransEffDate, 
      CloseBeg
    FROM dt_exp02
    WHERE 1 = 1
          AND TranType IN ('Cancellation', 'Policy Change') AND prevTranType = 'Policy Change'
          AND TransEffDate < prevTransEff AND JobCloseDate < prevTransEff
          AND TermActiveCount > 0
  )
  ,dt_oos1 AS 
  (
    SELECT 
      cn1.PolicyNumber,
      cn1.RiskNumber,
      cn1.TermNumber,
      cn1.ModelNumber,
      cn1.CloseBeg,
      min(cn2.CloseBeg) AS nextCloseBegMin
    FROM dt_oos AS cn1
      LEFT JOIN dt_oos AS cn2
      ON cn2.PolicyNumber = cn1.PolicyNumber
      AND cn2.TermNumber = cn1.TermNumber
      AND cn2.RiskNumber = cn1.RiskNumber
      AND cn2.ModelNumber > cn1.ModelNumber
    GROUP BY 
      cn1.PolicyNumber,
      cn1.RiskNumber,
      cn1.TermNumber,
      cn1.ModelNumber,
      cn1.CloseBeg
  ) 
  ,dt_oos2 AS 
  (
    SELECT 
      PolicyNumber,
      RiskNumber,
      TermNumber,
      COALESCE(oosPrevModelNum,1) AS oosMNBeg,
      -- ModelNumber AS oosMNEnd,
      ModelNumber - 1 AS oosMNEnd,
      oosCloseBeg
    FROM 
    (
      SELECT 
        PolicyNumber,
        RiskNumber,
        TermNumber,
        ModelNumber, 
        CloseBeg AS oosCloseBeg,
        LAG(ModelNumber,1)   OVER (PARTITION BY	PolicyNumber,
                                                TermNumber,
                                                RiskNumber
                                    ORDER BY	ModelNumber)						AS oosPrevModelNum
      FROM dt_oos1 
      WHERE CloseBeg < nextCloseBegMin OR nextCloseBegMin IS NULL
    )
  )
  SELECT 
    dt_exp02.PolicyNumber,
    dt_exp02.ProductType,
    dt_exp02.IsInactive,
    dt_exp02.PeriodEffDate,
    dt_exp02.PeriodEndDate,
    dt_exp02.JobNumber,
    dt_exp02.TranType,
    dt_exp02.TermNumber,
    dt_exp02.ModelNumber,
    dt_exp02.TransEffDate,
    dt_exp02.JobCloseDate,
    dt_exp02.CancelEffDate,
    dt_exp02.TNModified,
    dt_exp02.RiskNumber,
    dt_exp02.CloseBeg,
    dt_exp02.TermActiveCount,
    dt_exp02.ActiveCumSum,
    dt_exp02.InactiveCumSum,
    dt_exp02.MinActiveEffTermN,
    dt_exp02.prevTranType,
    dt_exp02.prevTransEff,
    dt_exp02.nextTranType,
    dt_exp02.nextTransEff,
    dt_exp02.nextCloseBeg,
    dt_exp02.nextIsInactive,
    dt_exp02.prevIsInactive,
    dt_exp02.TranBeg,
    dt_exp02.ExpBegCY,
    CASE   
        WHEN dt_exp02.ExpEndCY > dt_oos2.oosCloseBeg AND dt_exp02.ExpEndCY != dt_exp02.ExpBegCY THEN --IM: PolicyNumber = '55-015200' AND RiskNumber = 1 AND TermNumber = 3;PolicyNumber = '55-000342' AND RiskNumber = 2 AND TermNumber = 11
            IF(dt_exp02.ExpBegCY < dt_oos2.oosCloseBeg,dt_oos2.oosCloseBeg,dt_exp02.ExpBegCY)
        ELSE dt_exp02.ExpEndCY 
    END AS ExpEndCY,
    dt_exp02.WeBegCY,
    dt_exp02.WeEndCY,
    LAST_VALUE(ModelNumber IGNORE NULLS) OVER(PARTITION BY dt_exp02.PolicyNumber,dt_exp02.TermNumber,dt_exp02.RiskNumber 
                                              ORDER BY dt_exp02.CloseBegRank ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS LastMNValue
  FROM dt_exp02

    LEFT JOIN dt_oos2 
    --IM: PolicyNumber = '55-002306' AND RiskNumber = 5 AND TermNumber = 2;PolicyNumber = '55-004877' AND RiskNumber = 1 AND TermNumber = 4;cancel PolicyNumber = '55-007718' AND RiskNumber = 2 AND TermNumber = 5
    --BOP: PolicyNumber = '55-011018' AND RiskNumber = '1_1' AND TermNumber = 7;cancel PolicyNumber = '55-004619' AND RiskNumber = '1_1' AND TermNumber = 8
    ON dt_oos2.PolicyNumber = dt_exp02.PolicyNumber
    AND dt_oos2.TermNumber = dt_exp02.TermNumber
    AND dt_oos2.RiskNumber = dt_exp02.RiskNumber
    AND dt_exp02.ModelNumber BETWEEN dt_oos2.oosMNBeg  AND dt_oos2.oosMNEnd
);
CREATE OR REPLACE TEMP TABLE dt_exp04_a AS -- 0-1-0 sequence 
(
  WITH dt_oos_oio AS 
  (
    SELECT 
      dt_exp03.PolicyNumber,
      dt_exp03.RiskNumber,
      dt_exp03.TermNumber,
      dt_exp03.ModelNumber,
      LastMN.CloseBeg AS OOSCloseBeg
    FROM dt_exp03
      --IM/BOP: PolicyNumber = '55-015200' AND RiskNumber = '2_1' AND TermNumber = 3; PolicyNumber = '55-003637' AND RiskNumber = '1_1' AND TermNumber = 6
      INNER JOIN dt_exp03 AS LastMN
      ON LastMN.PolicyNumber = dt_exp03.PolicyNumber
      AND LastMN.TermNumber = dt_exp03.TermNumber
      AND LastMN.RiskNumber = dt_exp03.RiskNumber
      AND LastMN.ModelNumber = dt_exp03.LastMNValue
    WHERE 1 = 1
          AND dt_exp03.TranType = 'Policy Change' AND dt_exp03.prevTranType = 'Policy Change'
          AND dt_exp03.TransEffDate < dt_exp03.prevTransEff AND dt_exp03.JobCloseDate < dt_exp03.prevTransEff AND dt_exp03.prevIsInactive = 1 AND dt_exp03.IsInactive = 0
          AND LastMN.IsInactive = 0
          AND dt_exp03.TermActiveCount > 0
  )
  SELECT 
    dt_exp03.PolicyNumber,
    dt_exp03.ProductType,
    dt_exp03.IsInactive,
    dt_exp03.PeriodEffDate,
    dt_exp03.PeriodEndDate,
    dt_exp03.JobNumber,
    dt_exp03.TranType,
    dt_exp03.TermNumber,
    dt_exp03.ModelNumber,
    dt_exp03.TransEffDate,
    dt_exp03.JobCloseDate,
    dt_exp03.CancelEffDate,
    dt_exp03.TNModified,
    dt_exp03.RiskNumber,
    dt_exp03.CloseBeg,
    dt_exp03.TermActiveCount,
    dt_exp03.ActiveCumSum,
    dt_exp03.InactiveCumSum,
    dt_exp03.MinActiveEffTermN,
    dt_exp03.prevTranType,
    dt_exp03.prevTransEff,
    dt_exp03.nextTranType,
    dt_exp03.nextTransEff,
    dt_exp03.nextCloseBeg,
    dt_exp03.nextIsInactive,
    dt_exp03.prevIsInactive,
    dt_exp03.TranBeg,
    CASE 
      --IM: PolicyNumber = '55-012917' AND RiskNumber = 1 AND TermNumber = 5;PolicyNumber = '55-011192' AND RiskNumber = 1 AND TermNumber = 1;'55-007162',2,4
      --BOP: PolicyNumber = '55-006910' AND RiskNumber = '3_1' AND TermNumber = 5;PolicyNumber = '55-007039' AND RiskNumber = '1_1' AND TermNumber = 9
      WHEN dt_oos_oio.OOSCloseBeg IS NOT NULL THEN  dt_exp03.CloseBeg
      ELSE dt_exp03.ExpBegCY
    END AS ExpBegCY,
    ExpEndCY,
    CASE
      WHEN dt_oos_oio.OOSCloseBeg IS NOT NULL THEN IF(dt_exp03.CloseBeg < dt_oos_oio.OOSCloseBeg, dt_exp03.CloseBeg, dt_exp03.prevTransEff)--IM/BOP: PolicyNumber = '55-015200' AND RiskNumber = 2 AND TermNumber = 3
      ELSE dt_exp03.WeBegCY
    END AS WeBegCY,
    dt_exp03.WeEndCY,
    MIN(CASE WHEN IsInactive = 1 AND prevIsInactive = 0 THEN TransEffDate END) 
        OVER(PARTITION BY   dt_exp03.PolicyNumber,
                            dt_exp03.TermNumber,
                            dt_exp03.RiskNumber
              ORDER BY       dt_exp03.ModelNumber
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)              AS PriorMinAInActiveEff,
    MIN(CASE WHEN IsInactive = 0 THEN TransEffDate END) 
        OVER(PARTITION BY   dt_exp03.PolicyNumber,
                            dt_exp03.TermNumber,
                            dt_exp03.RiskNumber,
                            dt_exp03.InactiveCumSum
              ORDER BY       dt_exp03.ModelNumber
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)              AS GrpMinActiveEff
  FROM dt_exp03
    LEFT JOIN dt_oos_oio 
    ON dt_oos_oio.PolicyNumber = dt_exp03.PolicyNumber
    AND dt_oos_oio.TermNumber = dt_exp03.TermNumber
    AND dt_oos_oio.RiskNumber = dt_exp03.RiskNumber
    AND dt_oos_oio.ModelNumber = dt_exp03.ModelNumber
);
CREATE OR REPLACE TEMP TABLE dt_exp04_b AS  -- Active Inactive (1 0 1) Out of sequence on same date
(
  WITH dt_minAIClose AS
  (
    SELECT 
      PolicyNumber,
      TermNumber,
      RiskNumber,
      TransEffDate AS PriorMinAInActiveEff,
      MIN(JobCloseDate) AS MinClose
    FROM dt_exp04_a
    WHERE IsInactive = 1 AND prevIsInactive = 0 
    GROUP BY
      PolicyNumber,
      TermNumber,
      RiskNumber,
      TransEffDate
  )
  ,dt_i_oos AS
  (
    SELECT 
      dt_exp04_a.PolicyNumber,
      dt_exp04_a.TermNumber,
      dt_exp04_a.ModelNumber,
      dt_exp04_a.RiskNumber,
      dt_exp04_a.JobCloseDate,
      dt_exp04_a.TransEffDate,
      dt_inactive_oos.OOS_Eff,
      dt_inactive_oos.OOS_ModelNum,
      dt_inactive_oos.OOS_MinInactive,
      dt_inactive_oos.OOS_MinClose,
      COUNT(*) OVER(PARTITION BY PolicyNumber,TermNumber,RiskNumber,JobCloseDate) AS RowCnt 
      --IM: PolicyNumber = '55-001926' AND RiskNumber = 23 AND TermNumber = 10
      --BOP: PolicyNumber = '55-012165' AND RiskNumber = '8_1' AND TermNumber = 6;PolicyNumber = '55-010005' AND RiskNumber = '2_1' AND TermNumber = 1
    FROM dt_exp04_a
      INNER JOIN 
      (
        SELECT DISTINCT
          dt_exp04_a.PolicyNumber,
          dt_exp04_a.TermNumber,
          dt_exp04_a.ModelNumber AS OOS_ModelNum,
          dt_exp04_a.RiskNumber,
          dt_exp04_a.JobCloseDate,
          dt_exp04_a.TransEffDate AS OOS_Eff,
          dt_exp04_a.PriorMinAInActiveEff AS OOS_MinInactive,
          dt_minAIClose.MinClose AS OOS_MinClose
        FROM dt_exp04_a
          LEFT JOIN dt_minAIClose USING(PolicyNumber,TermNumber,RiskNumber,PriorMinAInActiveEff)
        WHERE dt_exp04_a.TermActiveCount > 0
              AND dt_exp04_a.IsInactive = 1 
              -- AND dt_exp04_a.TransEffDate < dt_exp04_a.GrpMinActiveEff 
              -- AND dt_exp04_a.TransEffDate <= dt_exp04_a.PriorMinAInActiveEff
            AND IF(dt_exp04_a.TransEFfDate < dt_exp04_a.MinActiveEffTermN,dt_exp04_a.MinActiveEffTermN,dt_exp04_a.TransEffDate) < dt_exp04_a.GrpMinActiveEff 
            --BOP: PolicyNumber = '55-010005' AND RiskNumber = '2_1' AND TermNumber = 1 (If condition added)
            AND IF(dt_exp04_a.TransEFfDate < dt_exp04_a.MinActiveEffTermN,dt_exp04_a.MinActiveEffTermN,dt_exp04_a.TransEffDate) <= dt_exp04_a.PriorMinAInActiveEff
              AND dt_exp04_a.TranType != 'Reinstatement' 
              --IM: PolicyNumber = '55-002181' AND RiskNumber = 2 AND TermNumber = 1
              --BOP: PolicyNumber = '55-003667' AND RiskNumber = '3_1' AND TermNumber = 5
      )dt_inactive_oos USING (PolicyNumber,TermNumber,RiskNumber,JobCloseDate)
    WHERE dt_exp04_a.ModelNumber <= dt_inactive_oos.OOS_ModelNum
  )
  SELECT 
    dt_exp04_a.PolicyNumber,
    dt_exp04_a.ProductType,
    dt_exp04_a.IsInactive,
    dt_exp04_a.PeriodEffDate,
    dt_exp04_a.PeriodEndDate,
    dt_exp04_a.JobNumber,
    dt_exp04_a.TranType,
    dt_exp04_a.TermNumber,
    dt_exp04_a.ModelNumber,
    dt_exp04_a.TransEffDate,
    dt_exp04_a.JobCloseDate,
    dt_exp04_a.CancelEffDate,
    dt_exp04_a.TNModified,
    dt_exp04_a.RiskNumber,
    dt_exp04_a.CloseBeg,
    dt_exp04_a.TermActiveCount,
    dt_exp04_a.ActiveCumSum,
    dt_exp04_a.InactiveCumSum,
    dt_exp04_a.MinActiveEffTermN,
    dt_exp04_a.prevTranType,
    dt_exp04_a.prevTransEff,
    dt_exp04_a.nextTranType,
    dt_exp04_a.nextTransEff,
    dt_exp04_a.nextCloseBeg,
    dt_exp04_a.nextIsInactive,
    dt_exp04_a.prevIsInactive,
    dt_exp04_a.TranBeg,
    CASE   
        WHEN dt_exp04_a.ModelNumber = dt_i_oos.OOS_ModelNum THEN dt_exp04_a.TranBeg
        ELSE dt_exp04_a.ExpBegCY
    END AS ExpBegCY,
    CASE   
        WHEN dt_i_oos.OOS_ModelNum IS NULL THEN dt_exp04_a.ExpEndCY
        WHEN dt_exp04_a.ModelNumber < dt_i_oos.OOS_ModelNum AND dt_exp04_a.TransEffDate > dt_i_oos.OOS_Eff THEN dt_exp04_a.ExpBegCY
        WHEN dt_exp04_a.ModelNumber = dt_i_oos.OOS_ModelNum THEN IF(dt_i_oos.OOS_MinInactive < dt_exp04_a.CloseBeg AND dt_i_oos.OOS_MinClose != dt_exp04_a.JobCloseDate,dt_i_oos.OOS_MinInactive,dt_exp04_a.ExpEndCY) 
        ELSE dt_exp04_a.ExpEndCY
    END AS ExpEndCY,
    CASE   
        WHEN dt_exp04_a.ModelNumber = dt_i_oos.OOS_ModelNum THEN dt_exp04_a.TranBeg
        ELSE dt_exp04_a.WeBegCY
    END AS WeBegCY,
    CASE   
        WHEN dt_i_oos.OOS_ModelNum IS NULL THEN dt_exp04_a.WeEndCY
        WHEN dt_exp04_a.ModelNumber < dt_i_oos.OOS_ModelNum AND dt_exp04_a.TransEffDate > dt_i_oos.OOS_Eff THEN dt_exp04_a.WeBegCY
        WHEN dt_exp04_a.ModelNumber = dt_i_oos.OOS_ModelNum THEN IF(dt_i_oos.OOS_MinInactive < dt_exp04_a.CloseBeg AND dt_i_oos.OOS_MinClose != dt_exp04_a.JobCloseDate,dt_i_oos.OOS_MinInactive,dt_exp04_a.WeEndCY) 
        ELSE dt_exp04_a.WeEndCY
    END AS WeEndCY
  FROM dt_exp04_a

    LEFT JOIN dt_i_oos
    ON dt_i_oos.PolicyNumber = dt_exp04_a.PolicyNumber 
    AND dt_i_oos.TermNumber = dt_exp04_a.TermNumber
    AND dt_i_oos.RiskNumber = dt_exp04_a.RiskNumber
    AND dt_i_oos.ModelNumber = dt_exp04_a.ModelNumber
    AND dt_i_oos.RowCnt > 1
);
CREATE OR REPLACE TEMP TABLE dt_exp05 AS  -- Exp > Period End
(
  SELECT 
    PolicyNumber,
    ProductType,
    IsInactive,
    PeriodEffDate,
    PeriodEndDate,
    JobNumber,
    TranType,
    TermNumber,
    ModelNumber,
    TransEffDate,
    JobCloseDate,
    CancelEffDate,
    TNModified,
    RiskNumber,
    CloseBeg,
    TermActiveCount,
    ActiveCumSum,
    InactiveCumSum,
    prevTranType,
    prevTransEff,
    nextTranType,
    nextTransEff,
    nextCloseBeg,
    nextIsInactive,
    prevIsInactive,
    TranBeg,
    ExpBegCY,
    CASE   
        WHEN ExpEndCY > PeriodEndDate THEN IF(ExpBegCY > PeriodEndDate,ExpBegCY,PeriodEndDate)
        ELSE ExpEndCY 
    END AS ExpEndCY,
    CASE 
        WHEN TranType = 'Cancellation' OR IsInactive = 1 THEN -1 
        ELSE 1 
    END AS CalendarYearMultiplier,
    WeBegCY,
    CASE   
        WHEN WeEndCY > PeriodEndDate THEN IF(WeBegCY > PeriodEndDate,WeBegCY,PeriodEndDate)
        ELSE WeEndCY 
    END AS WeEndCY
  FROM dt_exp04_b
);
  --need to be output (not sure if we need to create table)

SET output_table = `output_table`;
EXECUTE IMMEDIATE format("""
   CREATE OR REPLACE TABLE %s 
   AS (SELECT 
    PolicyNumber,
    ProductType,
    TermNumber,
    ModelNumber,
    RiskNumber,
    ExpBegCY AS CalendarYearBeginDate_Earned,
    ExpEndCY AS CalendarYearEndDate_Earned,
    CalendarYearMultiplier,
    WeBegCY AS CalendarYearBeginDate_Written,
    WeEndCY AS CalendarYearEndDate_Written, 
    DATE_DIFF(ExpEndCY, ExpBegCY, day) * CalendarYearMultiplier AS EarnedExposureDays,
    DATE_DIFF(WeEndCY, WeBegCY, day) * CalendarYearMultiplier AS WrittenExposureDays,
    SUM(DATE_DIFF(ExpEndCY, ExpBegCY, day) * CalendarYearMultiplier) 
            OVER(PARTITION BY   PolicyNumber,
                                TermNumber,
                                RiskNumber
                ORDER BY       ModelNumber
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)               AS EECumSum
    FROM `dt_exp05` 
  );
  """, output_table);

  END