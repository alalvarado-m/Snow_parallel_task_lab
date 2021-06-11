/****************************************************************************************************************************************************
Name: CREATE_TABLES

Parameters: 
  1- tables_to_create: This parameters defines the number of tables that are going to be created.

Description: This procedure creates the the number of tables defined by the parameter tables_to_create. 
The table name resulted is going to be: TASKS_LAB_[INCREMENTAL]
The fields of the table to be created are:
  1- id number
  2- task_name string
  3- comments string
  4- update_ts timestamp

How to use: CALL TASKS_LAB.CREATE_TABLES (4);
****************************************************************************************************************************************************/
CREATE OR REPLACE 
PROCEDURE CREATE_TABLES (tables_to_create float)
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
  
var iterator = 1;
while (iterator <= TABLES_TO_CREATE) {

  EXEC(`create or replace table TASKS_LAB.tasks_test_${iterator}(
                          id number identity start 1 increment 1,
                          task_name string,
                          comments string,
                          update_ts timestamp
                          )`);   
  iterator = iterator + 1;                         
}

return "Completed";

$$;
