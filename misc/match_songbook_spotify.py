import os
import sys
import re
from io import StringIO
import pandas as pd

# Add the parent directory to sys.path so we can import djlibman
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

import djlibman
from djlibman import djlib_config

# Set of iconic Great American Songbook performers to prioritize
CLASSIC_ARTISTS = {
    # Vocalists
    "frank sinatra", "ella fitzgerald", "nat king cole", "billie holiday", 
    "louis armstrong", "bing crosby", "sarah vaughan", "tony bennett", 
    "dinah washington", "julie london", "chet baker", "ray charles", 
    "judy garland", "peggy lee", "mel tormé", "mel torme", "fred astaire", 
    "dean martin", "sammy davis jr.", "bobby darin", "jo stafford", 
    "doris day", "rosemary clooney", "perry como", "vic damone", 
    "lena horne", "anita o'day", "nina simone", "johnny mathis", 
    "carmen mcrae", "abbey lincoln", "blossom dearie", "helen merrill", 
    "dinah shore", "keely smith", "margaret whiting", "jeri southern", 
    "mildred bailey", "lee wiley", "june christy", "chris connor", 
    "eartha kitt", "ethel waters", "al jolson", "cab calloway", 
    "fats waller", "louis jordan", "ella mae morse", "billy eckstine",
    "andy williams", "bobby short", "johnny hartman", "dick haymes",
    "barbra streisand", "diana krall", "julie andrews", "mitch miller",
    "gene kelly", "freddy martin", "vickie carr",
    "astrud gilberto", "joao gilberto", "antonio carlos jobim",
    
    # Additional classics to protect from high-popularity filters
    "andrews sisters", "the andrews sisters", "aretha franklin", "etta james",
    "bette midler", "marilyn monroe", "esther phillips", "carmen miranda",
    "kay kyser", "burt bacharach", "bacharach", "jimmy webb", "jule styne",
    "harry carroll", "lew pollack", "albert ammons", "pete johnson", "meade lux lewis",
    "pat boone", "the browns", "mario kostellani", "lionel belasco",
    
    # Famous Jazz Instrumentalists/Bandleaders
    "miles davis", "john coltrane", "bill evans", "oscar peterson", 
    "duke ellington", "count basie", "benny goodman", "artie shaw",
    "glenn miller", "tommy dorsey", "stan getz", "dave brubeck", 
    "erroll garner", "charlie parker", "thelonious monk",
    "coleman hawkins", "lester young", "ben webster", "art tatum",
    "sonny rollins", "jack teagarden", "art pepper", "dexter gordon",
    "gerry mulligan", "paul desmond", "ahmad jamal", "mccoy tyner",
    "wynton kelly", "red garland", "benny carter", "roy eldridge",
    "dizzy gillespie", "clifford brown", "lee morgan", "freddie hubbard",
    "hank mobley", "stanley turrentine", "wayne shorter"
}

def clean_title(title):
    # Remove smart quotes and normal quotes
    cleaned = title.replace('“', '').replace('”', '').replace('‘', '').replace('’', '')
    cleaned = re.sub(r'\(.*?\)', '', cleaned)
    cleaned = re.sub(r'".*?"', '', cleaned)
    return cleaned.strip()

def titles_match(query_title, track_title):
    # Normalize both by removing non-alphanumeric chars
    q = re.sub(r'\W+', '', query_title.lower())
    t = re.sub(r'\W+', '', track_title.lower())
    
    # Check for direct substring match
    if q in t or t in q:
        return True
        
    # Simplify by removing common words to handle things like "and" vs "&"
    def simplify(s):
        s = s.lower()
        s = re.sub(r'\band\b', '', s)
        s = re.sub(r'\bthe\b', '', s)
        s = re.sub(r'\W+', '', s)
        return s
        
    q_simple = simplify(query_title)
    t_simple = simplify(track_title)
    if q_simple in t_simple or t_simple in q_simple:
        return True
        
    return False

def score_track(track, song_title, composer):
    track_name = track['name']
    
    # Disqualify if the titles do not match at all
    if not titles_match(song_title, track_name):
        return 0
        
    popularity = track.get('popularity', 0)
    
    # Check if the artist matches a classic artist
    artist_names_str = str(track.get('artist_names', ''))
    track_artists = [name.strip().lower() for name in artist_names_str.split('|')]
    
    has_classic_artist = False
    for artist in track_artists:
        if artist in CLASSIC_ARTISTS:
            has_classic_artist = True
            break
            
    # Check if the artist matches the composer
    has_composer_match = False
    if composer:
        composers = [c.strip().lower() for c in re.split(r'[,&]|\band\b', str(composer))]
        for comp in composers:
            if comp and any(comp in artist for artist in track_artists):
                has_composer_match = True
                break
                
    # Disqualify modern pop collisions during search
    # (high popularity but not performed by a classic/protected artist or the composer)
    if popularity >= 45 and not has_classic_artist and not has_composer_match:
        return 0
        
    score = popularity
    
    # 2. Title matching details
    norm_query_title = re.sub(r'\W+', '', song_title.lower())
    norm_track_title = re.sub(r'\W+', '', track_name.lower())
    
    if norm_query_title == norm_track_title:
        score += 60
    elif norm_query_title in norm_track_title:
        score += 30
        
    # 3. Classic artist bonus
    if has_classic_artist:
        score += 100
        
    # 4. Composer/Artist match bonus
    if has_composer_match:
        score += 40
                
    return score

def get_composer_search_term(composer):
    if not composer or pd.isna(composer):
        return ""
    # Split by comma, & or 'and' to get individual composers
    parts = re.split(r'[,&]|\band\b', str(composer))
    first_composer = parts[0].strip()
    
    # Get last name or first name if single word
    words = first_composer.split()
    if len(words) > 1:
        return words[-1]
    return first_composer

def find_credible_spotify_matches(song_title, composer):
    cleaned_song = clean_title(song_title)
    
    # Track candidate matches to avoid duplicates
    # Key: spotify_id -> (track, score)
    candidate_matches = {}
    
    # Helper to process and score tracks from a search result DataFrame
    def process_tracks(df):
        if df.empty:
            return
        for _, track in df.iterrows():
            sp_id = track['spotify_id']
            if sp_id not in candidate_matches:
                score = score_track(track, song_title, composer)
                if score > 0:
                    candidate_matches[sp_id] = (track, score)

    # 1. Try structured track search
    query = f'track:"{cleaned_song}"'
    tracks_df = djlib_config.spotify.search(query, limit=50)
    process_tracks(tracks_df)
    
    # 2. Try general search
    query = cleaned_song
    tracks_df = djlib_config.spotify.search(query, limit=50)
    process_tracks(tracks_df)
        
    # 3. Try composer-specific fallback searches
    comp_term = get_composer_search_term(composer)
    if comp_term:
        fallback_query = f'track:"{cleaned_song}" {comp_term}'
        fallback_df = djlib_config.spotify.search(fallback_query, limit=50)
        process_tracks(fallback_df)
        
        fallback_query = f'{cleaned_song} {comp_term}'
        fallback_df = djlib_config.spotify.search(fallback_query, limit=50)
        process_tracks(fallback_df)
        
    # Sort matches by score in descending order
    sorted_matches = sorted(candidate_matches.values(), key=lambda x: x[1], reverse=True)
    
    # Return top 10 matches
    return sorted_matches[:10]

def main(limit_runs=None):
    csv_path = os.path.expanduser('~/Music/djlib/Great American Songbook.csv')
    output_path = os.path.expanduser('~/Music/djlib/Great American Songbook with Spotify IDs.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: Source CSV not found at {csv_path}")
        sys.exit(1)
        
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} songs from {csv_path}")
    
    songs_to_process = df.head(limit_runs) if limit_runs else df
    total = len(songs_to_process)
    print(f"Processing {total} songs...")
    
    output_rows = []
    
    for idx, row in songs_to_process.iterrows():
        song_title = row['Song title']
        composer = row['Composer(s)']
        lyricist = row['Lyricist(s)']
        year = row['Year']
        
        print(f"[{idx+1}/{total}] Searching for '{song_title}'...", end="", flush=True)
        
        matches = find_credible_spotify_matches(song_title, composer)
        
        if matches:
            print(f" Found {len(matches)} credible matches.")
            for track, score in matches:
                spotify_id = track['spotify_id']
                artist_name = track['artist_names'].replace('|', ', ')
                track_name = track['name']
                
                output_rows.append({
                    'Year': year,
                    'Song title': song_title,
                    'Composer(s)': composer,
                    'Lyricist(s)': lyricist,
                    'spotify_id': spotify_id,
                    'matched_track': track_name,
                    'matched_artist': artist_name,
                    'match_score': score
                })
        else:
            print(" No matches found.")
            output_rows.append({
                'Year': year,
                'Song title': song_title,
                'Composer(s)': composer,
                'Lyricist(s)': lyricist,
                'spotify_id': "",
                'matched_track': "",
                'matched_artist': "",
                'match_score': 0
            })
            
        # Periodic saving to prevent progress loss
        if (idx + 1) % 20 == 0:
            temp_df = pd.DataFrame(output_rows)
            temp_df.to_csv(output_path, index=False)
            print(f"--- Saved progress up to song {idx+1} ---")
            
    # Final save
    final_df = pd.DataFrame(output_rows)
    final_df.to_csv(output_path, index=False)
    print(f"Done! Saved results to {output_path}")

if __name__ == '__main__':
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Running test pass with limit of {limit} songs.")
        except ValueError:
            pass
    main(limit_runs=limit)
