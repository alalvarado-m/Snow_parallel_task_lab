/****************************************************************************************************************************************************

-- Master task which creates dynamic tasks - Compared to PR_REFRESH_LOOKUP

****************************************************************************************************************************************************/

CREATE OR REPLACE 
PROCEDURE PR_TASK_DEMO_V1 (EXECUTION_TAG VARCHAR, NUMBER_OF_TABLES float)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
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


tables_list =
   EXEC(`select table_name from information_schema.tables where table_name like 'TASKS_TEST%' LIMIT ${NUMBER_OF_TABLES}
    `);

while (tables_list.next()) {

    [table_name] = tables_list.FETCH();

    EXEC(`CREATE OR REPLACE TASK ${table_name}
            WAREHOUSE = JORGE_MOBILIZE_WH 
            SCHEDULE = '1 MINUTE' 
            ALLOW_OVERLAPPING_EXECUTION = true
          AS 
            call insert_task_time('${table_name}', '${EXECUTION_TAG}')
    `);
    
    EXEC(`ALTER TASK ${table_name} RESUME
    `);

}
  // Put entry in task completed status to performed busy waits when necessary.
  EXEC(`INSERT INTO TASKS_LAB.TASK_COMPLETED_STATUS(EXECUTION_TAG, TASK_NAME, COMMENTS, EXECUTION_START, EXECUTION_FINISH) VALUES('${EXECUTION_TAG}', 'Master', 'Execution of Master task', '${start_time}'::timestamp_ntz, current_timestamp)
  `);

return "Completed";
$$;
