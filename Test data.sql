
-- Just for lab use. Create a table to insert dummy data. 
create or replace table TASKS_LAB.TEST_DATA(
    id number identity start 1 increment 1,
    task_name string,
    comments string,
    update_ts timestamp
);

/****************************************************************************************************************************************************
Name: FILL_TABLE

Parameters:
1- ROWS_TO_INSERT: number of rows to be created in the table specified as the second parameter.

Description: This procedure insert the numbers of rows defined as parameter in the table defined as parameter too.

How to use: call FILL_TABLE(5);
****************************************************************************************************************************************************/
CREATE OR REPLACE 
PROCEDURE FILL_TABLE (ROWS_TO_INSERT FLOAT)
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
  
  var insert_iterator = 1;

  var sql_cmd = `insert into TASKS_LAB.TEST_DATA(task_name,comments,update_Ts) VALUES`;

  while (insert_iterator <= ROWS_TO_INSERT) {

    //To do, insert dinamically in the columns of the table specified.
    // Now we are asuming the columns of the table. 
    sql_cmd += ` ('tasks_test_${insert_iterator}', 'This comment is updated by task: tasks_test_${insert_iterator}',current_timestamp)`

    insert_iterator = insert_iterator + 1;  

    if ( insert_iterator <= ROWS_TO_INSERT ) {
      sql_cmd += `,`;  
    }    
    
  }

  EXEC(sql_cmd);

return "Completed";
$$;