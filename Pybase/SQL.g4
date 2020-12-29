grammar SQL;

EqualOrAssign: '=';
Less: '<';
LessEqual: '<=';
Greater: '>';
GreaterEqual: '>=';
NotEqual: '<>';

Identifier: [a-zA-Z_] [a-zA-Z_0-9]*;
Integer: [0-9]+;
String:  '\'' (~'\'')* '\'';
Float: ('-')? [0-9]+ '.' [0-9]*;
Whitespace: [ \t\n\r]+ -> skip;
Annotation: '-' '-' (~';')+;

program
    : statement* EOF
    ;

statement
    : system_statement ';'
    | db_statement ';'
    | table_statement ';'
    | index_statement ';'
    | alter_statement ';'
    | Annotation ';'
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
    : 'CREATE' 'TABLE' Identifier '(' field_list ')'                    # create_table
    | 'DROP' 'TABLE' Identifier                                         # drop_table
    | 'DESC' Identifier                                                 # describe_table
    | 'INSERT' 'INTO' Identifier 'VALUES' value_lists                   # insert_into_table
    | 'DELETE' 'FROM' Identifier 'WHERE' where_and_clause               # delete_from_table
    | 'UPDATE' Identifier 'SET' set_clause 'WHERE' where_and_clause     # update_table
    | 'SELECT' selector 'FROM' identifiers 'WHERE' where_and_clause     # select_table
    ;

index_statement
    : 'CREATE' 'INDEX' Identifier 'ON' Identifier '(' identifiers ')'           # create_index
    | 'DROP' 'INDEX' Identifier                                                 # drop_index
    | 'ALTER' 'TABLE' Identifier 'ADD' 'INDEX' Identifier '(' identifiers ')'   # alter_add_index
    | 'ALTER' 'TABLE' Identifier 'DROP' 'INDEX' Identifier                      # alter_drop_index
    ;

alter_statement
    : 'ALTER' 'TABLE' Identifier 'ADD' field                                    # alter_table_add
    | 'ALTER' 'TABLE' Identifier 'DROP' Identifier                              # alter_table_drop
    | 'ALTER' 'TABLE' Identifier 'CHANGE' Identifier field                      # alter_table_change
    | 'ALTER' 'TABLE' Identifier 'RENAME' 'TO' Identifier                       # alter_table_rename
    | 'ALTER' 'TABLE' Identifier 'DROP' 'PRIMARY' 'KEY' (Identifier)?           # alter_table_drop_pk
    | 'ALTER' 'TABLE' Identifier 'DROP' 'FOREIGN' 'KEY' Identifier              # alter_table_drop_foreign_key
    | 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' Identifier 'PRIMARY' 'KEY' '(' identifiers ')'      # alter_table_add_pk
    | 'ALTER' 'TABLE' Identifier 'ADD' 'CONSTRAINT' Identifier 'FOREIGN' 'KEY' '(' identifiers ')' 'REFERENCES' Identifier '(' identifiers ')'  # alter_table_add_foreign_key
    ;

field_list
    : field (',' field)*
    ;

field
    : Identifier type_ ('NOT' 'NULL')? ('DEFAULT' value)?                               # normal_field
    | 'PRIMARY' 'KEY' '(' identifiers ')'                                               # primary_key_field
    | 'FOREIGN' 'KEY' '(' Identifier ')' 'REFERENCES' Identifier '(' Identifier ')'     # foreign_key_field
    ;

type_
    : 'INT' '(' Integer ')'
    | 'VARCHAR' '(' Integer ')'
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
    | Float
    | 'NULL'
    ;

where_and_clause
    : where_clause ('AND' where_clause)*
    ;

where_clause
    : column operator expression
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
    : Identifier EqualOrAssign value (',' Identifier EqualOrAssign value)*
    ;

selector
    : '*'
    | column (',' column)*
    ;

identifiers
    : Identifier (',' Identifier)*
    ;

operator
    : EqualOrAssign
    | Less
    | LessEqual
    | Greater
    | GreaterEqual
    | NotEqual
    ;