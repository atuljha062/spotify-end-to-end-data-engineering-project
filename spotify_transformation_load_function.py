import json
import spotipy
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

def album(data):
    album_dict = {'album_id':[],'album_name':[],'album_release_date':[],'album_total_tracks':[],'album_external_url':[]}
    
    for item in data['items']:
        album_id = item['track']['album']['id']
        album_name = item['track']['album']['name']
        album_release_date = item['track']['album']['release_date']
        album_total_tracks = item['track']['album']['total_tracks']
        album_external_url = item['track']['album']['external_urls']['spotify']
        
        album_dict['album_id'].append(album_id)
        album_dict['album_name'].append(album_name)
        album_dict['album_release_date'].append(album_release_date)
        album_dict['album_total_tracks'].append(album_total_tracks)
        album_dict['album_external_url'].append(album_external_url)
        
    return album_dict;
    
def artist(data):
    artist_dict = {'artist_id':[],'artist_name':[],'artist_external_url':[]}
    
    for item in data['items']:
        for artist in item['track']['artists']:
            artist_id = artist['id']
            artist_name = artist['name']
            artist_external_url = artist['href']
            
            artist_dict['artist_id'].append(artist_id)
            artist_dict['artist_name'].append(artist_name)
            artist_dict['artist_external_url'].append(artist_external_url)
            
    return artist_dict
    
def songs(data):
    song_dict = {'song_id':[],'song_name':[],'song_duration':[],'song_url':[],'song_popularity':[],'song_added':[],'album_id':[],'artist_id':[]}
    
    for item in data['items']:
        song_id = item['track']['id']
        song_name = item['track']['name']
        song_duration = item['track']['duration_ms']
        song_url = item['track']['external_urls']['spotify']
        song_popularity = item['track']['popularity']
        song_added = item['added_at']
        album_id = item['track']['album']['id']
        artist_id = item['track']['artists'][0]['id']
        
        song_dict['song_id'].append(song_id)
        song_dict['song_name'].append(song_name)
        song_dict['song_duration'].append(song_duration)
        song_dict['song_url'].append(song_url)
        song_dict['song_popularity'].append(song_popularity)
        song_dict['song_added'].append(song_added)
        song_dict['album_id'].append(album_id)
        song_dict['artist_id'].append(artist_id)
        
    return song_dict

def lambda_handler(event, context):
    
    # creating bucket and key variable
    Bucket = 'spotify-etl-snowflake-project'
    Key = 'raw_data/to_process/'
    
    # Creating s3 object
    s3 = boto3.client('s3')
    
    spotify_data = []
    spotify_keys = []
    
    # Looping over all the files in raw_data location
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        
        # Check if the file key contains json file
        if file_key.split('/')[-1].find(".json") != -1:
            
            # Getting the response from S3
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            
            content = response['Body']
            
            # Convert the content file into json file
            jsonObject = json.loads(content.read())
            
            # Storing multiple files of data and thir key in list
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
            
    for data in spotify_data:
        
        # calling functions and getting the data
        album_dict = album(data)
        artist_dict = artist(data)
        song_dict = songs(data)
            
        # Creating dataframe from dictionaries
        
        album_df = pd.DataFrame.from_dict(album_dict)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_dict)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_dict)
        song_df = song_df.drop_duplicates(subset=['song_id'])
        
        # Converting non datetime columns into datetime columns
        
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        # Putting all of the data in transformed folder of S3
        
        # Album
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'
        
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        
        s3.put_object(
            Bucket = Bucket,
            Key = album_key,
            Body = album_content
            )
            
        # Artist
        
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        
        s3.put_object(
            Bucket = Bucket,
            Key = artist_key,
            Body = artist_content
            )
            
        #Song
        
        song_key = 'transformed_data/song_data/song_transformed_' + str(datetime.now()) + '.csv'
        
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        
        s3.put_object(
            Bucket = Bucket,
            Key = song_key,
            Body = song_content
            )
            
            
    # Copying the data from to_processed to processed in raw_data folder and delete the files from to_processed
        
    s3_resource = boto3.resource('s3')
        
    for key in spotify_keys:
        
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()