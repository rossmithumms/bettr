SELECT
  DAT.VALUE_STR
  , DAT.VALUE_NUM
  , DAT.VALUE_DT
FROM
  APP_DQHI_DEV.BETTR_TEST_DATA DAT
WHERE
  DAT.VALUE_NUM <= &VALUE_NUM
--;
--END