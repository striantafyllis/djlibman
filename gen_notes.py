
import sys

notes = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
alt_notes = [None, 'Db', None, 'Eb', None, None, 'Gb', None, 'Ab', None, 'Bb', None]

assert len(notes) == 12

def note_index(note_name):
    try:
        index = notes.index(note_name)
    except ValueError:
        try:
            index = alt_notes.index(note_name)
        except ValueError:
            raise Exception("Invalid note '%s'" % note_name)

    return index

def next_note(note_name):
    next_index = (note_index(note_name)+1) %12

    return notes[next_index]

def midi_number_to_note(midi_number, yamaha=False):
    assert midi_number >= 0 and midi_number <= 127

    # MIDI 0 is C-1 in conventional notation and C-2 in Yamaha notation
    octave = midi_number // 12 - 1 - (1 if yamaha else 0)

    note_number = midi_number % 12

    note = notes[note_number] + '%s' % octave

    return note

def note_to_midi_number(note, yamaha=False):
    assert len(note) >= 2

    if note[1] == '#' or note[1] == 'b':
        note_name = note[:2]
    else:
        note_name = note[:1]

    octave_str = note[len(note_name):]

    octave = int(octave_str) + 1 + (1 if yamaha else 0)

    return octave * 12 + note_index(note_name)

middle_a = 'A4'
middle_a_midi_number = note_to_midi_number(middle_a)

middle_a_frequency = 440.0

def midi_number_to_frequency(midi_number):
    assert midi_number >= 0 and midi_number <= 127

    return middle_a_frequency * 2 ** (1 / 12 * (midi_number - middle_a_midi_number))

def note_to_frequency(note):
    return midi_number_to_frequency(note_to_midi_number(note))

def print_notes_freqs():
    for midi_number in range(128):
        frequency = midi_number_to_frequency(midi_number)
        if frequency < 20.0:
            continue

        note = midi_number_to_note(midi_number)
        note_yamaha = midi_number_to_note(midi_number, True)

        print('%d %s %s %.3f' % (midi_number, note, note_yamaha, frequency))

    return

def main():
    print_notes_freqs()
    return 0

if __name__ == '__main__':
    sys.exit(main())


