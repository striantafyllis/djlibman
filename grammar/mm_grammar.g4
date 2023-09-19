
grammar mm_grammar;

r: CREATE PLAYLIST ;

create_playlist_stmt:
    CREATE platform? PLAYLIST ID ;

platform: REKORDBOX | SPOTIFY | YOUTUBE ;

REKORDBOX: [Rr][Ee][Kk][Oo][Rr][Dd][Bb][Oo][Xx] ;
SPOTIFY: [Ss][Pp][Oo][Tt][Ii][Ff][Yy] ;
YOUTUBE: [Yy][Oo][Uu][Tt][Uu][Bb][Ee] ;

CREATE: [Cc][Rr][Ee][Aa][Tt][Ee] ;
PLAYLIST: [Pp][Ll][Aa][Yy][Ll][Ii][Ss][Tt] ;
FROM: [Ff][Rr][Oo][Mm] ;
QUERY: [Qq][Ee][Rr][Yy] ;

ID: [A-Za-z_][A-Za-z_0-9]* ;

DOUBLE_QUOTED_STRING : '"' ('\\"'|.)*? '"' ;

SINGLE_QUOTED_STRING: '"' ('\\"'|.)*? '"' ;

WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines, \r (Windows)

