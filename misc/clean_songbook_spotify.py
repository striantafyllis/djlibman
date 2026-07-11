import os
import re
import pandas as pd

PROTECTED_ARTISTS = {
    "sinatra", "fitzgerald", "cole", "holiday", "armstrong", "crosby", "vaughan",
    "bennett", "washington", "london", "baker", "charles", "garland", "lee",
    "torme", "tormé", "astaire", "martin", "davis", "darin", "stafford", "day",
    "clooney", "como", "damone", "horne", "o'day", "simone", "mathis", "mcrae",
    "lincoln", "dearie", "merrill", "shore", "smith", "whiting", "southern",
    "bailey", "wiley", "christy", "connor", "kitt", "waters", "jolson", "calloway",
    "waller", "jordan", "morse", "eckstine", "williams", "short", "hartman",
    "haymes", "streisand", "krall", "andrews", "miller", "kelly", "freddy martin",
    "carr", "gilberto", "jobim", "coltrane", "evans", "peterson", "ellington",
    "basie", "goodman", "shaw", "glenn miller", "dorsey", "getz", "brubeck",
    "garner", "parker", "monk", "hawkins", "young", "webster", "tatum",
    
    # Additional classics to protect
    "andrews sisters", "aretha franklin", "etta james", "bette midler",
    "marilyn monroe", "esther phillips", "carmen miranda", "kay kyser",
    "bacharach", "jimmy webb", "jule styne", "harry carroll", "lew pollack",
    "vic damone", "albert ammons", "pete johnson", "meade lux lewis"
}

def should_keep(artist_name, score):
    if pd.isna(artist_name) or not artist_name:
        return False
        
    # High scores are already verified classic matches (which got the +100 bonus)
    if score >= 150:
        return True
        
    # Low scores (< 110) are typically correct old recordings that have very low popularity
    if score < 110:
        return True
        
    # For borderline scores (110-149), check if the artist is in our protected list
    artist_lower = artist_name.lower()
    
    for protected in PROTECTED_ARTISTS:
        if protected in artist_lower:
            return True
            
    return False

def main():
    csv_path = os.path.expanduser('~/Music/djlib/Great American Songbook with Spotify IDs.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV not found at {csv_path}")
        return
        
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} matches for post-processing...")
    
    cleaned_count = 0
    for idx, row in df.iterrows():
        artist = row['matched_artist']
        score = row['match_score']
        
        if pd.notna(row['spotify_id']) and not should_keep(artist, score):
            print(f"Cleaning modern collision at index {idx}: '{row['Song title']}' matched to '{row['matched_track']}' by {artist} (Score: {score})")
            df.at[idx, 'spotify_id'] = ""
            df.at[idx, 'matched_track'] = ""
            df.at[idx, 'matched_artist'] = ""
            df.at[idx, 'match_score'] = 0
            cleaned_count += 1
            
    # Save the cleaned CSV
    df.to_csv(csv_path, index=False)
    print(f"Finished! Cleaned {cleaned_count} modern collisions. Saved to {csv_path}")

if __name__ == '__main__':
    main()
