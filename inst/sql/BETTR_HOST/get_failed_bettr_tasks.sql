SELECT
  *
FROM
  V_BETTR_TASK
WHERE
  LAST_STATUS IN (
    20   -- Failed
    , 21 -- Failed, No Retry
  )