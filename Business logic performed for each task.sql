/****************************************************************************************************************************************************
Business logic, actual task t be executed.
****************************************************************************************************************************************************/
create or replace procedure TASKS_LAB.insert_task_time(TASK_NAME VARCHAR, EXECUTION_TAG VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS caller
AS
$$ 
var _RS,_ROWS,ACTIVITY_COUNT=0,ROW_COUNT=0, MESSAGE_TEXT='', SQLCODE=0, SQLSTATE='', ERROR_HANDLERS;
var LAST_SQL='';
var fetch = (count,rows,stmt) => count && rows.next() && Array.apply(null,Array(stmt.getColumnCount())).map((_,i) => rows.getColumnValue(i+1));
var INTO = function() { 
  if (ROW_COUNT) return fetch(ROW_COUNT,_ROWS,_RS); else return [];
};

var EXEC = function(stmt,binds,noCatch) {
    LAST_SQL = stmt;
    ROW_COUNT=0;
    ACTIVITY_COUNT=0;
    MESSAGE_TEXT='';
    SQLSTATE='';

    var fixBind = (arg) => arg == undefined ? null : arg instanceof Date ? arg.toISOString() : arg;
    binds = binds ? binds.map(fixBind) : (binds || []);
    try {
        _RS = snowflake.createStatement({sqlText: stmt, binds: binds});
        _ROWS = _RS.execute();
        ROW_COUNT = _RS.getRowCount();  ACTIVITY_COUNT = _RS.getNumRowsAffected();
        _ROWS.statement = _RS;
      /* local closures*/ 
      var __RS = _RS, __ROWS = _ROWS;
      _ROWS.FETCH = function() { return Array.apply(null,Array(__RS.getColumnCount())).map((_,i) => __ROWS.getColumnValue(i+1));}
      return _ROWS;
    }
    catch (error) {
        MESSAGE_TEXT = error.message;
        SQLCODE = error.code;
        SQLSTATE = error.state;
        var errmsg = `${SQLCODE}:${SQLSTATE}:${MESSAGE_TEXT} SQL: ${stmt} ARGS: [${(binds||[]).join(',')}]`;
        var newError = new Error(errmsg);
        newError.state = error.state;
        throw newError;
    }
    
}
/*****************************************************************************************************************************************************/
  
  var start_time;
  EXEC(`SELECT CURRENT_TIMESTAMP::timestamp_ntz::varchar
    `);
  [start_time] = INTO();
 
  var existence_number;
  EXEC(`Select count(*) 
        from TASK_COMPLETED_STATUS
        where TASK_NAME = ?
    `, [TASK_NAME]);
  [existence_number] = INTO();

  if(existence_number === 0)
  {
    /********************************************************************************/
    //Start business logic
    
    EXEC(`DELETE FROM TASKS_LAB.${TASK_NAME}
      `);

    EXEC(`INSERT INTO TASKS_LAB.${TASK_NAME}(id, task_name, comments, update_Ts) 
          SELECT * FROM TASKS_LAB.TEST_DATA;
      `);

    //End business logic
    /********************************************************************************/

    // Put entry in task run history to track and delete completed tasks
    EXEC(`INSERT INTO TASKS_LAB.TASK_RUN_HISTORY VALUES('${TASK_NAME}',current_timestamp)
    `);

    // Put entry in task completed status to performed busy waits when necessary.
    EXEC(`INSERT INTO TASKS_LAB.TASK_COMPLETED_STATUS(EXECUTION_TAG, TASK_NAME, COMMENTS, EXECUTION_START, EXECUTION_FINISH) VALUES('${EXECUTION_TAG}', '${TASK_NAME}', 'Execution of task ${TASK_NAME}', '${start_time}'::timestamp_ntz, current_timestamp)
    `);
  }

  return "Completed";
$$