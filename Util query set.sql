CALL PR_TASK_DEMO_V1('Test 200 tables V1');

SELECT * FROM TASK_COMPLETED_STATUS;

DELETE FROM TASK_COMPLETED_STATUS;

SELECT * FROM TASK_RUN_HISTORY;

DELETE FROM TASK_RUN_HISTORY;

SHOW TASKS;

select NAME, QUERY_TEXT, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
from table(information_schema.task_history(RESULT_LIMIT => 1000))
where query_text like '%Test 200 tables V1%';

select *
from table(information_schema.task_history(RESULT_LIMIT => 1000))
where query_text like '%Test 200 tables V1%';


select COUNT(*), MIN(EXECUTION_START), MAX(EXECUTION_FINISH), DATEDIFF(MINUTE, MIN(EXECUTION_START), MAX(EXECUTION_FINISH))
from TASK_COMPLETED_STATUS
where EXECUTION_TAG like '%Test 200 tables V1%';

select *
from table(information_schema.QUERY_history(RESULT_LIMIT => 1000));

