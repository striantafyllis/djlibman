
import sys

def get_user_choice(prompt: str, options: list[str] = ['yes', 'no']):
    """Allows the user to choose among a number of options by typing any unambiguous prefix
    (usually the first letter) of an option"""
    while True:
        sys.stdout.write(prompt + ' (' + '/'.join(options) + ') > ')
        sys.stdout.flush()

        reply = sys.stdin.readline().strip()

        possible_options = [option for option in options if option.upper().startswith(reply.upper())]

        if len(possible_options) == 1:
            return possible_options[0]
        elif len(possible_options) == 0:
            sys.stdout.write('Reply not recognized; try again.')
        else:
            sys.stdout.write('Reply is ambiguous; try again.')
