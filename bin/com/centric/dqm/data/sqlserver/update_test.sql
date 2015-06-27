UPDATE [dqm.[test] SET
  [failure_flag] = {1}
, [failure_case_ct] = {2}
, [success_case_ct] = {3}
, [error_flag] = {4}
, [test_error_number] = {5}
, [test_error_message] = {6}
, [expected_error_number] = {7}
, [expected_error_message] = {8}
, [actual_error_number] = {9}
, [actual_error_message] = {10}
WHERE
  [test_uid] = {0}
;