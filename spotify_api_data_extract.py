import json
import os
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    
    # Creating connection with Spotify API and creating spotify object
    client_credentials_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Getting the playlist id from the playlist link
    playlist_url = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_uri = playlist_url.split('/')[-1]
    
    # Getting playlist data using playlist_tracks function of spotipy
    spotify_data = sp.playlist_tracks(playlist_uri)
    
    # Creating s3 object to store the raw data in S3
    
    s3 = boto3.client('s3')
    
    file_name = 'spotify_raw_' + str(datetime.now()) + '.json'
    
    s3.put_object(
        Bucket = 'spotify-etl-snowflake-project',
        Key = 'raw_data/to_process/' + file_name,
        Body = json.dumps(spotify_data)
        )
    
    