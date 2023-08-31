
import sys
import antlr4
from grammar.HelloLexer import HelloLexer
from grammar.HelloParser import HelloParser


def main(argv):
    while True:
        sys.stdout.write('> ')
        sys.stdout.flush()
        input = sys.stdin.readline()

        if input == '':
            break

        input = input.strip()
        if input == '':
            continue

        antlr_input = antlr4.InputStream(input)

        lexer = HelloLexer(antlr_input)
        stream = antlr4.CommonTokenStream(lexer)
        parser = HelloParser(stream)
        tree = parser.r()
        print(tree.toStringTree(recog=parser))

if __name__ == '__main__':
    main(sys.argv)
