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
    , &BETTR_LIB_TEST_MODE BETTR_LIB_TEST_MODE -- unit test mode enabled or not
  FROM
    DUAL
)

-- SELECT * FROM GET_PARAMS;

 , GET_MATCHING_TASKS AS (
   SELECT
     BT.*
   FROM
     V_BETTR_TASK BT
     INNER JOIN GET_PARAMS GP
       ON (
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
         OR BT.OPT_START_DT >= TO_DATE(GP.OPT_START_DT, 'YYYY-MM-DD HH24:MI:SS')
       ) AND (
         GP.OPT_END_DT IS NULL
         OR BT.OPT_END_DT <= TO_DATE(GP.OPT_END_DT, 'YYYY-MM-DD HH24:MI:SS')
       ) AND (
         (
           GP.OPT_CACHE_EXPIRY_MINS IS NULL
         ) OR (
           BT.OPT_CACHE_EXPIRY_MINS <= 0
             AND GP.OPT_CACHE_EXPIRY_MINS <= 0
         ) OR (
           BT.OPT_CACHE_EXPIRY_MINS > 0
             AND GP.OPT_CACHE_EXPIRY_MINS > 0
             AND BT.OPT_CACHE_EXPIRY_MINS >= GP.OPT_CACHE_EXPIRY_MINS
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
       ) AND (
         (
           GP.BETTR_LIB_TEST_MODE = 1
           AND BT.BETTR_TASK_GIT_PROJECT LIKE '__bt_%'
         )
         OR (
           GP.BETTR_LIB_TEST_MODE = 0
           AND BT.BETTR_TASK_GIT_PROJECT NOT LIKE '__bt_%'
         )
       )
 )
 
 SELECT * FROM GET_MATCHING_TASKS
--;