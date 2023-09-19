
import sys
from datetime import date

def get_user_choice(batch_mode: bool, prompt: str, options: list[str] = ['yes', 'no']):
    """Allows the user to choose among a number of options by typing any unambiguous prefix
    (usually the first letter) of an option"""
    assert len(options) > 0

    if batch_mode:
        return options[0]

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

def infer_type(value):
    """Converts a string to a typed value (int, float etc.) if possible"""

    if not isinstance(value, str):
        return value

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    try:
        return date.fromisoformat(value)
    except ValueError:
        pass

    return value

