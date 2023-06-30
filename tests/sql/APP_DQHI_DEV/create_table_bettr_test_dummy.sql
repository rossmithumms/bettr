-- 2023-06-01, rossmith@med.umich.edu
-- Use this to create the test data set for bettr.
-- Connection: APP_DQHI_DEV

CREATE TABLE BETTR_TEST_DUMMY (
  KEY NUMBER GENERATED BY DEFAULT AS IDENTITY
  , VALUE_STR VARCHAR(255) NOT NULL
  , VALUE_NBR NUMBER
  , VALUE_DT TIMESTAMP
)
--;
-- END