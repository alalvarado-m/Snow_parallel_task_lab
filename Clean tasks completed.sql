/****************************************************************************************************************************************************
Util operations
****************************************************************************************************************************************************/

-- Run table which tracks the task runs

create or replace table TASKS_LAB.task_run_history (task_name string, task_run_time timestamp);

-- Stream over task run

create or replace stream TASKS_LAB.task_run_history_stream on table task_run_history append_only=true;

-- Proc called by task to cleanup dynamic tasks.

CREATE OR REPLACE 
PROCEDURE TASKS_LAB.PR_CLEANUP_TASKS_V1()
RETURNS VARCHAR 
LANGUAGE JAVASCRIPT
as
$$

task_list = snowflake.execute({sqlText: "select task_name from TASKS_LAB.task_run_history"})

while (task_list.next()) {

    task_name = task_list.getColumnValue(1)    

    snowflake.execute({sqlText: "delete from TASKS_LAB.task_run_history where task_name = '" + task_name +  "'"})
    snowflake.execute({sqlText: "drop task " + task_name })

}
$$
;

-- Task over Stream
create or replace task TASKS_LAB.task_cleanup_run_tasks 
warehouse = <WAREHOUSE_NAME>
schedule = '1 MINUTE'
when
    system$stream_has_data('task_run_history_stream')
as
    call PR_CLEANUP_TASKS_V1();

-- Start the clen uo task 

alter task task_cleanup_run_tasks resume;

-- Create control table 
CREATE OR REPLACE TABLE TASKS_LAB.TASK_COMPLETED_STATUS(
    ID NUMBER identity start 1 increment 1,
    EXECUTION_TAG STRING,
    TASK_NAME STRING,
    COMMENTS STRING,
    EXECUTION_START TIMESTAMP,
    EXECUTION_FINISH TIMESTAMP 
);