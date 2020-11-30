grammar SQL;

Operator: '=' | '<>' | '<=' | '>=' | '<' | '>' ;
Identifier: [a-zA-Z_] [a-zA-Z_0-9]*;
Integer: [0-9]+;
String:  '\'' (~'\'')* '\'';
Whitespace: [ \t\n\r]+ -> skip;

program
    : statement* EOF
    ;

statement
    : system_statement ';'
    | db_statement ';'
    | table_statement ';'
    | index_statement ';'
    | alter_statement ';'
    ;

system_statement
    : 'SHOW' 'DATABASES'
    ;

db_statement
    : 'CREATE' 'DATABASE' Identifier    # create_db
    | 'DROP' 'DATABASE' Identifier      # drop_db
    | 'USE' Identifier                  # use_db
    | 'SHOW' 'TABLES'                   # show_tables
    ;

table_statement
    : 'CREATE' 'TABLE' Identifier '(' field_list ')'
    | 'DROP' 'TABLE' Identifier
    | 'DESC' Identifier
    | 'INSERT' 'INTO' Identifier 'VALUES' value_lists
    | 'DELETE' 'FROM' Identifier 'WHERE' where_and_clause
    | 'UPDATE' Identifier 'SET' set_clause 'WHERE' where_and_clause
    | 'SELECT' selector 'FROM' identifiers 'WHERE' where_and_clause
    ;

index_statement
    : 'CREATE' 'INDEX' Identifier 'ON' Identifier '(' identifiers ')'
    | 'DROP' 'INDEX' Identifier
    | 'ALTER' 'TABLE' Identifier 'ADD' 'INDEX' Identifier '(' identifiers ')'
    | 'ALTER' 'TABLE' Identifier 'DROP' 'INDEX' Identifier
    ;

alter_statement
    : 'ALTER' 'TABLE' Identifier 'ADD' field
    | 'ALTER' 'TABLE' Identifier 'DROP' Identifier
    | 'ALTER' 'TABLE' Identifier 'CHANGE' Identifier field
    | 'ALTER' 'TABLE' Identifier 'RENAME' 'TO' Identifier
    | 'ALTER' 'TABLE' Identifier 'ADD' 'PRIMARY' 'KEY' '(' identifiers ')'
    | 'ALTER' 'TABLE' Identifier 'DROP' 'PRIMARY' 'KEY' (Identifier)?
    | 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' Identifier 'PRIMARY' 'KEY' '(' identifiers ')'
    | 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' Identifier 'FOREIGN' 'KEY' '(' identifiers ')' 'REFERENCES' Identifier '(' identifiers ')'
    | 'ALTER' 'TABLE' Identifier 'DROP' 'FOREIGN' 'KEY' Identifier
    ;

field_list
    : field (',' field)*
    ;

field
    : Identifier type_ ('NOT' 'NULL')? ('DEFAULT' value)?
    | 'PRIMARY' 'KEY' '(' identifiers ')'
    | 'FOREIGN' 'KEY' '(' Identifier ')' 'REFERENCES' Identifier '(' Identifier ')'
    ;

type_
    : 'INT' (Integer)?
    | 'VARCHAR' (Integer)?
    | 'DATE'
    | 'FLOAT'
    ;

value_lists
    : value_list (',' value_list)*
    ;

value_list
    : '(' value (',' value)* ')'
    ;

value
    : Integer
    | String
    | 'NULL'
    ;

where_and_clause
    : where_clause ('AND' where_clause)*
    ;

where_clause
    : column Operator expression
    | column 'IS' ('NOT')? 'NULL'
    ;

column
    : (Identifier '.')? Identifier
    ;


expression
    : value
    | column
    ;

set_clause
    : Identifier '=' value
    | set_clause ',' Identifier '=' value
    ;

selector
    : '*'
    | column (',' column)*
    ;

identifiers:
    Identifier (',' Identifier)*
    ;
