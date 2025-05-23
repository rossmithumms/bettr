WITH GET_TASKS_AND_EXPIRY_MINS AS (
  SELECT
    BT.*
    , round (
      extract ( day from SYSDATE - BT.LAST_TASK_STARTED_DT ) * 14400 +
      extract ( hour from SYSDATE - BT.LAST_TASK_STARTED_DT ) * 60  +
      extract ( minute from SYSDATE - BT.LAST_TASK_STARTED_DT )
    ) LAST_TASK_STARTED_MINS
  FROM
    V_BETTR_TASK BT
  WHERE
    BT.BETTR_TASK_GIT_PROJECT = &BETTR_TASK_GIT_PROJECT
    AND BT.BETTR_TASK_GIT_BRANCH = &BETTR_TASK_GIT_BRANCH
    AND (
     (
      &INCL_LIVE_REFRESH = 1
      AND BT.OPT_CACHE_EXPIRY_MINS > 0
    ) OR (
      &INCL_HISTORICAL_REFRESH = 1
      AND BT.OPT_CACHE_EXPIRY_MINS <= 0
    )
  )
)

, GET_JOBS_BY_TASK_FLAGS AS (
  SELECT
    GTAEM.BETTR_TASK_JOB_PRIORITY
    , GTAEM.BETTR_TASK_JOB_ID
    , SUM(
      CASE WHEN GTAEM.OPT_CACHE_EXPIRY_MINS > 0
        AND GTAEM.LAST_TASK_STARTED_MINS >= GTAEM.OPT_CACHE_EXPIRY_MINS
        THEN 1 END
      ) HAS_EXPIRED_CACHE_TASKS
    , SUM(
      CASE WHEN GTAEM.LAST_STATUS = 10 AND GTAEM.LAST_TASK_STARTED_MINS >= 5 THEN 1 END
      ) HAS_EXPIRED_IN_PROGRESS_TASKS
    , SUM(
      CASE WHEN GTAEM.LAST_TASK_STARTED_MINS <= 1 THEN 1 END
      ) HAS_IN_PROGRESS_TASKS
    , SUM(
      CASE WHEN GTAEM.LAST_STATUS IN (0, 20) THEN 1 END
      ) HAS_INACTIVE_INCOMPLETE_TASKS
  FROM
    GET_TASKS_AND_EXPIRY_MINS GTAEM
  GROUP BY
    GTAEM.BETTR_TASK_JOB_PRIORITY
    , GTAEM.BETTR_TASK_JOB_ID
)

, GET_NEXT_BETTER_TASK_JOB_ID AS (
  SELECT DISTINCT
    FIRST_VALUE(GJBTF.BETTR_TASK_JOB_ID) OVER (
      ORDER BY
        GJBTF.BETTR_TASK_JOB_PRIORITY
        , GJBTF.BETTR_TASK_JOB_ID
    ) NEXT_BETTER_TASK_JOB_ID
  FROM
    GET_JOBS_BY_TASK_FLAGS GJBTF
  WHERE
    (
      GJBTF.HAS_INACTIVE_INCOMPLETE_TASKS > 0
      OR GJBTF.HAS_EXPIRED_CACHE_TASKS > 0
    ) AND (
      GJBTF.HAS_IN_PROGRESS_TASKS IS NULL
      OR GJBTF.HAS_IN_PROGRESS_TASKS = 0
    )
)

SELECT
  GTAEM.*
FROM
  GET_TASKS_AND_EXPIRY_MINS GTAEM
  INNER JOIN GET_NEXT_BETTER_TASK_JOB_ID GNBTJI
    ON GTAEM.BETTR_TASK_JOB_ID = GNBTJI.NEXT_BETTER_TASK_JOB_ID
ORDER BY
  GTAEM.BETTR_TASK_JOB_PRIORITY
  , GTAEM.BETTR_TASK_JOB_ID
  , GTAEM.BETTR_TASK_SORT
--;