WITH a as (
                                             SELECT count(*) as record_count,
                                                                {partition_field} as check_partition_field_a
                                             FROM `{project}.{dataset_name}.{table_name}`
                                             WHERE {partition_field} = {dag_exec_date}
                                             GROUP BY {partition_field}),

                                       b as (
                                             SELECT count(*) as incomplete_key_count,
                                                                {partition_field} as check_partition_field_b
                                             FROM `{project}.{dataset_name}.{table_name}`
                                             WHERE BusinessKey IS NULL
                                                   OR BusinessKey=''
                                             GROUP BY {partition_field})

                                  SELECT '{task_name}' as task_name
                                       , '{dataset_name}' as dataset_name
                                       , '{table_name}' as table_name
                                       , {loop_num} as iteration_loop
                                       , record_count
                                       , incomplete_key_count
                                       , {dag_exec_date} as dag_execution_date
                                     , CURRENT_TIMESTAMP() as update_time
                                  FROM a LEFT JOIN b
                                  ON check_partition_field_a = check_partition_field_b
                                  WHERE check_partition_field_a = DATE("{{{{ ds }}}}")
                                  GROUP BY record_count, incomplete_key_count