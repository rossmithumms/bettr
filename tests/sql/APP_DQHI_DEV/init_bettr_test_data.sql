ALTER TABLE APP_DQHI_DEV.BETTR_TEST_DATA
  ADD (
  KEY NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY
  , AUDIT_INSERT_DT DATE DEFAULT ON NULL SYSDATE
  )
;;;
CREATE VIEW V_BETTR_TEST_DATA (
  KEY
  , VALUE_STR
  , VALUE_NUM
  , VALUE_DT
)

AS (
  SELECT
    KEY BETTR_TEST_DATA_KEY
    , "value_str"
    , "value_num"
    , "value_dt"
  FROM
    APP_DQHI_DEV.BETTR_TEST_DATA
)
