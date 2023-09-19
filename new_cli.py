
"""
Command-line interface functions, including parsing user input.
"""

import time

import antlr4
from grammar.MusicManagementGrammarLexer import MusicManagementGrammarLexer
from grammar.MusicManagementGrammarParser import MusicManagementGrammarParser
from grammar.MusicManagementGrammarVisitor import MusicManagementGrammarVisitor

from OLD import google_sheet, library_organizer
from utils import *

class ExitException(Exception):
    pass

class StmtNode(object):
    def execute(self):
        raise Exception('Not implemented')

class PlaylistNode(object):
    platform: str
    name: str

    def __init__(self, platform, name):
        self.platform = platform
        self.name = name
        return

    def __str__(self):
        return '%s PLAYLIST "%s"' % (self.platform, self.name)

class CreatePlaylistNode(StmtNode):
    target: PlaylistNode
    source: PlaylistNode

    def __init__(self, target, source):
        self.target = target
        self.source = source

    def __str__(self):
        return 'CREATE %s FROM %s' % (self.target, self.source)

class Visitor(MusicManagementGrammarVisitor):
    def aggregateResult(self, aggregate, nextResult):
        if aggregate is None:
            return nextResult
        if nextResult is None:
            return aggregate

        if not isinstance(aggregate, list):
            return [aggregate, nextResult]
        else:
            return aggregate + [nextResult]


    def visitIdentifier(self, node:MusicManagementGrammarParser.IdentifierContext):
        symbol = node.getChild(0).symbol

        if symbol.type == MusicManagementGrammarLexer.DOUBLE_QUOTED_STRING:
            return symbol.text[1:-1]
        else:
            return symbol.text

    def visitPlatform(self, node:MusicManagementGrammarParser.PlatformContext):
        platform_type = node.getChild(0).symbol.type

        if platform_type == MusicManagementGrammarLexer.REKORDBOX:
            return 'REKORDBOX'
        if platform_type == MusicManagementGrammarLexer.SPOTIFY:
            return 'SPOTIFY'
        if platform_type == MusicManagementGrammarLexer.YOUTUBE:
            return 'YOUTUBE'

        assert False

    def visitPlaylist_identifier(self, node:MusicManagementGrammarParser.Playlist_identifierContext):
        children = self.visitChildren(node)

        if len(children) == 1:
            return PlaylistNode('REKORDBOX', children[0])
        elif len(children) == 2:
            return PlaylistNode(children[0], children[1])
        else:
            assert False

    def visitCreate_playlist_stmt(self, node:MusicManagementGrammarParser.Create_playlist_stmtContext):
        children = self.visitChildren(node)

        assert len(children) == 2

        return CreatePlaylistNode(target=children[0], source=children[1])


def interpret_statement(statement: str):
    antlr_input = antlr4.InputStream(statement)

    lexer = MusicManagementGrammarLexer(antlr_input)
    stream = antlr4.CommonTokenStream(lexer)
    parser = MusicManagementGrammarParser(stream)

    tree = parser.statement()

    print(tree.toStringTree(recog=parser))

    stmt_node: StmtNode = tree.accept(Visitor())

    print('Executing statement: %s' % stmt_node)

    return None


def cli_loop(
        batch_mode: bool,
        input_fh,
        rekordbox_state: library_organizer.RekordboxState,
        sheet: google_sheet.GoogleSheet,
        playlist_dir: str
):
    print('NEW CLI: Ready to accept statements')
    fail_on_exception = (input_fh != sys.stdin)

    while (True):
        if input_fh == sys.stdin:
            sys.stdout.write('> ')
            sys.stdout.flush()
        statement = input_fh.readline()

        if statement == '':
            # EOF
            break

        statement = statement.strip()
        if statement == '':
            continue

        try:
            interpret_statement(statement)
        except Exception as e:
            if isinstance(e, ExitException):
                print('Goodbye!')
                break

            sys.stdout.flush()
            sys.stderr.write('Statement "%s" failed: %s\n' % (statement, e))
            sys.stderr.flush()
            time.sleep(0.1)
            print()
            if fail_on_exception:
                raise e
            continue


def main():
    # cli_loop(None, sys.stdin, None, None, None)
    interpret_statement('create spotify playlist blah from rekordbox playlist blah')
    return 0

if __name__ == '__main__':
    sys.exit(main())
