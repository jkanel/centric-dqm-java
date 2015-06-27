INSERT INTO [dqm].[test_case] (
  [test_uid]
, [measure_name]
, [precision]
, [expected_value]
, [expected_text]
, [actual_value]
, [actual_text]
, [result_variance]
, [result_variance_rate]
, [allowed_variance]
, [allowed_variance_rate]
, [failure_flag]
, [grain_01_name]
, [grain_01_text]
, [grain_02_name]
, [grain_02_text]
, [grain_03_name]
, [grain_03_text]
, [grain_04_name]
, [grain_04_text]
, [grain_05_name]
, [grain_05_text]
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
, {14}
, {15}
, {16}
, {17}
, {18}
, {19}
, {20}
, {21}
, CURRENT_TIMESTAMP
)
;