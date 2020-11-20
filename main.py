import pandas as pd
import requests
import psycopg2

# CREDENTIALS
CLIENT_ID = '4ae4c39d96574aff818d*****'
CLIENT_SECRET = '0ba53f9d5ce3489481de47*****'

AUTH_URL = 'https://accounts.spotify.com/api/token'

# -------------------------------------------------------------------------------

# POST
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})

# convert the response to JSON
auth_response_data = auth_response.json()

# save the access token
access_token = auth_response_data['access_token']

# -------------------------------------------------------------------------------

# HEADER TOKEN
headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}

# -------------------------------------------------------------------------------

# base URL of all Spotify API endpoints
BASE_URL = 'https://api.spotify.com/v1/'

# Track ID from the URI
artist_id_list = {'5sWHDYs0csV6RS48xBl0tH', '1Xyo4u8uXC1ZmMpatF05PJ', '36QJpDe2go2KgaRleHCDTp', '26VFTg2z8YR0cCuwLzESi2', '6qqNVTkY8uBg9cP3Jd7DAH'} #For artists
#artist_id_list = {'5sWHDYs0csV6RS48xBl0tH'}

# actual GET request with proper header
data_df_artist = []
data_df_tracks = []
origin = "Spotify API"
for artist_id in artist_id_list:

    # Get response artist and convert to json
    artist = requests.get(BASE_URL + 'artists/' + artist_id, headers=headers).json()
    # Change the name of 'id' to 'artist_id'
    artist.update({
        'artist_id': artist['id'],
        'origin_from': origin
    })

    #--------------------------------------------

    # Get response albums from artist and convert to json
    albums = requests.get(BASE_URL + 'artists/' + artist_id + '/albums', headers=headers).json()
    print('Albums')
    print(albums)
    # Save names to verify if is reapted
    albums_aux = []

    for album in albums['items']:
        album_name = album['name']

        # here's a hacky way to skip over albums we've already grabbed
        trim_name = album_name.split('(')[0].strip()
        if trim_name.upper() in albums: # or int(album['release_date'][:4]) > 1983:
            continue
        albums_aux.append(trim_name.upper())  # use upper() to standardize

        # this takes a few seconds so let's keep track of progress
        print(album_name)

        # pull all tracks from this album
        tracks = requests.get(BASE_URL + 'albums/' + album['id'] + '/tracks', headers=headers).json()
        tracks = tracks['items']

        print(tracks)

        for track in tracks:
            # get audio features (key, liveness, danceability, ...)
            f = requests.get(BASE_URL + 'audio-features/' + track['id'],
                             headers=headers)
            f = f.json()

            # combine with album info
            f.update({
                'track_id': f['id'],
                'album': album_name,
                'release_date': album['release_date'],
                'track_number': track['track_number'],
                'popularity': artist['popularity'],
                'artist_name': artist['name'],
                'genres': artist['genres'],
                'name': track['name'],
                'type': track['type'],
                'origin_from': origin
            })

            data_df_tracks.append(f)

    data_df_artist.append(artist)

print(data_df_artist)
print(data_df_tracks)


# -------------------------------------------------------------------------------

# DATA TO DB

# Artists
data_to_db_artist = pd.DataFrame(data_df_artist)
data_to_db_artist = data_to_db_artist[["artist_id", "name", "popularity", "type", "uri", "origin_from"]]
# Tracks
data_to_db_tracks = pd.DataFrame(data_df_tracks)
data_to_db_tracks = data_to_db_tracks[["track_id", "name", "artist_name", "album", "track_number", "popularity", "uri", "release_date", "genres", "type", "origin_from"]]
data_to_db_tracks['release_date'] = pd.to_datetime(data_to_db_tracks['release_date'])

print(data_to_db_artist)

def execute_sql(sql):
    connection = {}
    try:
        # Connection parameters to data base
        connection = psycopg2.connect(user="postgres",
                                      password="*********",
                                      host="127.0.0.1",
                                      port="5000",
                                      database="spotify_db")
        print(connection)
        # Creating cursor
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n\n")

        print("Executing a command(s): \n"+sql)
        cursor.execute(sql)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            connection.commit()
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def create_tables():
    commands = (
        """
        CREATE TABLE artists (
            artist_id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            popularity INTEGER,
            uri VARCHAR(255),
            type VARCHAR(255),
            origin_from VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()                                
        )
        """,
        """ CREATE TABLE tracks (
            track_id VARCHAR(255) PRIMARY KEY,                
            name VARCHAR(255),
            artist_name VARCHAR(255),
            album VARCHAR(255),
            track_number INTEGER,
            popularity INTEGER,
            uri VARCHAR(255),
            release_date DATE,
            genres VARCHAR(255),
            type VARCHAR(255),
            origin_from VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """
    )

    for command in commands:
        execute_sql(command)

    return commands

def insert_data(table_name, data):
    connection = {}
    try:
        # Connection parameters to data base
        connection = psycopg2.connect(user="postgres",
                                      password="cristhi@n",
                                      host="127.0.0.1",
                                      port="5000",
                                      database="spotify_db")
        print(connection)
        # Creating cursor
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n\n")

        # creating column list for insertion
        cols = ",".join([str(i) for i in data.columns.tolist()])

        # Insert DataFrame recrds one by one.
        for i, row in data.iterrows():
            sql = "INSERT INTO {} (".format(table_name) + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cursor.execute(sql, tuple(row))

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            connection.commit()
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

execute_sql("DROP TABLE artists, tracks")
create_tables()
insert_data("ARTISTS", data_to_db_artist)
insert_data("TRACKS", data_to_db_tracks)
