CREATE VIEW V_BETTR_TEST_DATA (
  VALUE_STR
  , VALUE_NUM
  , VALUE_DT
)

AS (
  SELECT
    "value_str"
    , "value_num"
    , "value_dt"
  FROM
    APP_DQHI_DEV.BETTR_TEST_DATA
)
