
grammar mm_grammar;

r: CREATE PLAYLIST ;


CREATE: [Cc][Rr][Ee][Aa][Tt][Ee] ;
PLAYLIST: [Pp][Ll][Aa][Yy][Ll][Ii][Ss][Tt] ;

ID: [A-Za-z_][A-Za-z_0-9]* ;

DOUBLE_QUOTED_STRING : '"' ('\\"'|.)*? '"' ;

SINGLE_QUOTED_STRING: '"' ('\\"'|.)*? '"' ;

WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines, \r (Windows)

