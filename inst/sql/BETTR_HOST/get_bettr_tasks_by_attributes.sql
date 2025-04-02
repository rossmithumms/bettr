WITH GET_PARAMS AS (
  SELECT
    &BETTR_TASK_GIT_PROJECT BETTR_TASK_GIT_PROJECT -- Regular expression match
    , &BETTR_TASK_JOB_COMMENT BETTR_TASK_JOB_COMMENT -- Regular expression match
    , &BETTR_TASK_NAME BETTR_TASK_NAME -- Regular expression match
    , &OPT_START_DT OPT_START_DT -- LTE match
    , &OPT_END_DT OPT_END_DT -- GTE match
    , &OPT_CACHE_EXPIRY_MINS OPT_CACHE_EXPIRY_MINS -- Negative or positive match
    , &OPT_NUMBER_LIST OPT_NUMBER_LIST -- Regular expression match
    , &OPT_CHAR_LIST OPT_CHAR_LIST -- Regular expression match
    , &LAST_STATUS LAST_STATUS -- Regular expression match
    , &LAST_ERROR LAST_ERROR -- Regular expression match
  FROM
    DUAL
)

, GET_MATCHING_TASKS AS (
  SELECT
    BT.*
  FROM
    V_BETTR_TASK BT
    INNER JOIN GET_PARAMS GP
      ON (
      GP.BETTR_TASK_GIT_PROJECT IS NOT NULL
      OR GP.BETTR_TASK_JOB_COMMENT IS NOT NULL
      OR GP.BETTR_TASK_NAME IS NOT NULL
      OR GP.OPT_START_DT IS NOT NULL
      OR GP.OPT_END_DT IS NOT NULL
      OR GP.OPT_CACHE_EXPIRY_MINS IS NOT NULL
      OR GP.OPT_NUMBER_LIST IS NOT NULL
      OR GP.OPT_CHAR_LIST IS NOT NULL
      OR GP.LAST_STATUS IS NOT NULL
      OR GP.LAST_ERROR IS NOT NULL
      ) AND (
        GP.BETTR_TASK_GIT_PROJECT IS NULL
        OR REGEXP_LIKE(
          BT.BETTR_TASK_GIT_PROJECT
          , GP.BETTR_TASK_GIT_PROJECT
        )
      ) AND (
        GP.BETTR_TASK_JOB_COMMENT IS NULL
        OR REGEXP_LIKE(
          BT.BETTR_TASK_JOB_COMMENT
          , GP.BETTR_TASK_JOB_COMMENT
        )
      ) AND (
        GP.BETTR_TASK_NAME IS NULL
        OR REGEXP_LIKE(
          BT.BETTR_TASK_NAME
          , GP.BETTR_TASK_NAME
        )
      ) AND (
        GP.OPT_START_DT IS NULL
        OR BT.OPT_START_DT <= TO_DATE(GP.OPT_START_DT, 'YYYY-MM-DD HH24:MI:SS')
      ) AND (
        GP.OPT_END_DT IS NULL
        OR BT.OPT_END_DT >= TO_DATE(GP.OPT_END_DT, 'YYYY-MM-DD HH24:MI:SS')
      ) AND (
        (
          BT.OPT_CACHE_EXPIRY_MINS <= 0
            AND GP.OPT_CACHE_EXPIRY_MINS <= 0
        ) OR (
          BT.OPT_CACHE_EXPIRY_MINS > 0
            AND GP.OPT_CACHE_EXPIRY_MINS > 0
        )
      ) AND (
        GP.OPT_NUMBER_LIST IS NULL
        OR REGEXP_LIKE(
          BT.OPT_NUMBER_LIST
          , GP.OPT_NUMBER_LIST
        )
      ) AND (
        GP.OPT_CHAR_LIST IS NULL
        OR REGEXP_LIKE(
          BT.OPT_CHAR_LIST
          , GP.OPT_CHAR_LIST
        )
      ) AND (
        GP.LAST_STATUS IS NULL
        OR REGEXP_LIKE(
          BT.LAST_STATUS
          , GP.LAST_STATUS
        )
      ) AND (
        GP.LAST_ERROR IS NULL
        OR REGEXP_LIKE(
          BT.LAST_ERROR
          , GP.LAST_ERROR
        )
      )
)

SELECT * FROM GET_MATCHING_TASKS
--;