UPDATE t SET
t.test_case_purge_dtm = {1}
FROM
[dqm].[test] t
WHERE
t.test_case_purge_dtm IS NULL
AND t.test_begin_dtm <= DATEADD(day,-1*{0}, CURRENT_TIMESTAMP)
;

DELETE tc FROM
[dqm].[test_case] tc
WHERE EXISTS (
  SELECT 1 FROM [dqm].[test] t WHERE
  t.test_uid = tc.test_uid
  AND t.test_case_purge_dtm <= {1}
)
;