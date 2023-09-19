
grammar MusicManagementGrammar;

statement:
      exit_stmt
    | create_playlist_stmt
    ;

exit_stmt: EXIT | QUIT | X | Q ;

create_playlist_stmt:
    CREATE playlist_identifier FROM playlist_identifier;

playlist_identifier:
    platform? PLAYLIST identifier ;

platform: REKORDBOX | SPOTIFY | YOUTUBE ;

identifier:
    IDENTIFIER |
    DOUBLE_QUOTED_STRING |
    EXIT |
    QUIT |
    X |
    Q
    ;

REKORDBOX: [Rr][Ee][Kk][Oo][Rr][Dd][Bb][Oo][Xx] ;
SPOTIFY: [Ss][Pp][Oo][Tt][Ii][Ff][Yy] ;
YOUTUBE: [Yy][Oo][Uu][Tt][Uu][Bb][Ee] ;

CREATE: [Cc][Rr][Ee][Aa][Tt][Ee] ;
PLAYLIST: [Pp][Ll][Aa][Yy][Ll][Ii][Ss][Tt] ;
FROM: [Ff][Rr][Oo][Mm] ;
QUERY: [Qq][Uu][Ee][Rr][Yy] ;
EXIT: [Ee][Xx][Ii][Tt] ;
QUIT: [Qq][Uu][Ii][Tt] ;
X: [Xx] ;
Q: [Qq] ;

IDENTIFIER: [A-Za-z_][A-Za-z_0-9]* ;

DOUBLE_QUOTED_STRING : '"' ('\\"'|.)*? '"' ;

SINGLE_QUOTED_STRING: '"' ('\\"'|.)*? '"' ;

WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines, \r (Windows)

