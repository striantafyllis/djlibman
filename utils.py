
import sys
from datetime import date

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

def infer_type(str):
    """Converts a string to a typed value (int, float etc.) if possible"""

    try:
        return int(str)
    except ValueError:
        pass

    try:
        return float(str)
    except ValueError:
        pass

    try:
        return date.fromisoformat(str)
    except ValueError:
        pass

    return str

