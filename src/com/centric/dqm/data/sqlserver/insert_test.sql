INSERT INTO [dqm].[test] (
  [test_uid]
, [scenario_uid]
, [test_dtm]
, [modularity]
, [modulus]
, [failure_case_ct]
, [success_case_ct]
, [case_failure_rate]
, [allowed_case_failure_rate]
, [failure_flag]
, [error_flag]
, [test_error_message]
, [expected_error_message]
, [actual_error_message]
, [create_dtm]
)
VALUES
(
  {0}
, {1}
, {2}
, {3}
, {4}
, {5}
, {6}
, {7}
, {8}
, {9}
, {10}
, {11}
, {12}
, {13}
, CURRENT_TIMESTAMP
)
;