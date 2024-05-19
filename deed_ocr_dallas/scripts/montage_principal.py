import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import re
import tqdm
import numpy as np
from geopy.geocoders import GoogleV3
from googlemaps.client import Client
from PIL import Image, ImageDraw, ImageFont
import moviepy.editor as mpy
import librosa
import os
import youtube_dl
import concurrent.futures
import time
import requests
from io import BytesIO


def normalize_subdivision_name(name):
    if not name:
        return None  # Return None if the input is None or empty

    # Convert to lowercase
    name = name.lower()
    
    # Handle common abbreviations and numeral formats
    name = re.sub(r'\bph\b', 'phase', name)
    name = re.sub(r'\binst\b', 'installment', name)
    name = re.sub(r'\bsec\b', 'section', name)
    name = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', name)  # Convert '1st', '2nd' to '1', '2'
    name = re.sub(r'(\d+)-([a-z])', r'\1\2', name)  # Convert '5-A' to '5A'
    name = re.sub(r'(\d+)[a-z]', r'\1', name)  # Convert '5a' to '5'

    # Additional normalization rules can be added here
    return name.strip()

# Function to standardize the LEGAL_2 fields
def standardize_legal2(legal_2):
    if pd.isna(legal_2):  # Check if legal_2 is NaN
        return None

    legal_2 = str(legal_2).strip().upper()  # Convert to string in case it's not, and then standardize
    legal_2 = legal_2.replace("LOT", "LT")
    legal_2 = legal_2.replace("BLOCK", "BLK")
    legal_2 = legal_2.replace(" ", "")
    return legal_2 if "BLK" in legal_2 else legal_2 + " BLK"  # Assume missing block is a generic block

def get_street_view_image(coords):
    lat, lon = coords
    try:
        # Using the static street view API endpoint with coordinates
        street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=640x640&location={lat},{lon}&key=YOUR_API_KEY"
        response = requests.get(street_view_url)
        if response.status_code == 200:
            return response.content  # Return image content directly or save to file and return file path
        else:
            print(f"Failed to retrieve street view image: {response.status_code}")
    except Exception as e:
        print(f"Error retrieving street view image for {lat}, {lon}: {e}")
    return None

# Parallelize the overlaying of text on street view images
def overlay_text(args):
    image_url, date, value = args
    if image_url:
        try:
            response = requests.get(image_url)
            if response.status_code == 200:
                img = Image.open(BytesIO(response.content))
                draw = ImageDraw.Draw(img)
                font = ImageFont.truetype('arial.ttf', 36)
                text = f"SOLD\n{date}\n${value}"
                text_bbox = draw.textbbox((0, 0), text, font=font)
                text_width, text_height = text_bbox[2] - text_bbox[0], text_bbox[3] - text_bbox[1]
                img_width, img_height = img.size
                angle = 45
                text_position = (img_width - text_width - 50, img_height - text_height - 50)
                draw.text(text_position, text, font=font, fill=(255, 0, 0))
                img = img.rotate(angle, expand=True)
                return img
        except Exception as e:
            print(f"Failed to process image from {image_url}: {e}")
    return None

def robust_get_street_view_image(address_components):
    try:
        return get_street_view_image(address_components)
    except requests.exceptions.RequestException as e:
        print(f"Network error for address {address_components}: {e}")
        return None  # Return None or a default image path if necessary

def format_address(st_num, st_name, city):
    # Properly capitalize the street name and city
    formatted_st_name = ' '.join(word.capitalize() for word in st_name.lower().split())
    formatted_city = city.capitalize()

    # Form the full address string, including state but excluding ZIP code
    full_address = f"{st_num} {formatted_st_name}, {formatted_city}, TX"
    return full_address

# Geocode addresses to obtain latitude and longitude coordinates
geolocator = GoogleV3(api_key='AIzaSyBGQ-044-SURjTIuACZXtW7wk-_58vCsEI')

# Retrieve street view images using the Google Street View Static API
gmaps = Client(key='AIzaSyBGQ-044-SURjTIuACZXtW7wk-_58vCsEI')

# Establish a connection to the database
engine = create_engine('sqlite:///deed_data.db')

# Load the deed data
deeds_df = pd.read_sql_query("SELECT * FROM deeds WHERE max_dollar_value IS NOT NULL ORDER BY max_dollar_value DESC LIMIT 90", engine)
print("Top 90 high value deeds loaded")

print("Loaded deeds data")

# Convert recorded_date to datetime immediately after loading
deeds_df['recorded_date'] = pd.to_datetime(deeds_df['recorded_date'], errors='coerce')

deeds_df['subdivision_normalized'] = deeds_df['subdivision'].apply(normalize_subdivision_name)
deeds_df['legal_2_standardized'] = deeds_df['lot_number'] + ' ' + deeds_df['block_number'].fillna('')
deeds_df['legal_2_standardized'] = deeds_df['legal_2_standardized'].apply(standardize_legal2)

# Load parcel data into a regular GeoDataFrame if Dask GeoPandas is problematic
parcels_gdf = gpd.read_parquet('Parcel.parquet')
print("Loaded parcels data")

parcels_gdf['subdivision_normalized'] = parcels_gdf['LEGAL_1'].apply(normalize_subdivision_name)
parcels_gdf['legal_2_standardized'] = parcels_gdf['LEGAL_2'].apply(standardize_legal2)

print("Starting merge...")

# Merge on standardized columns
#merged_gdf = parcels_gdf.merge(deeds_df, left_on=['subdivision_normalized', 'legal_2_standardized'], right_on=['subdivision_normalized', 'legal_2_standardized'])
# Perform the merge with a progress bar from tqdm
with tqdm.tqdm(total=len(deeds_df)) as pbar:
    for chunk in np.array_split(deeds_df, 10):  # Splitting deeds_df into 10 chunks
        temp_merged = parcels_gdf.merge(chunk, how='inner', on=['subdivision_normalized', 'legal_2_standardized'])
        if 'merged_gdf' in locals():
            merged_gdf = pd.concat([merged_gdf, temp_merged])
        else:
            merged_gdf = temp_merged
        pbar.update(len(chunk))

print("Merge completed")

# Convert to a GeoDataFrame and ensure CRS consistency
merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry='geometry', crs=parcels_gdf.crs)
merged_gdf['centroid'] = merged_gdf.geometry.centroid

# Sort by date for animation
merged_gdf.sort_values('recorded_date', inplace=True)

# Check if there are enough entries to sample from
if len(merged_gdf) >= 90:
    merged_gdf = merged_gdf.sample(n=90, random_state=42)
else:
    print(f"Warning: Only {len(merged_gdf)} properties available, proceeding with all.")

print(f"Selected {len(merged_gdf)} unique properties for processing.")

# Get the range of dates in the database
min_date = merged_gdf['recorded_date'].min()
max_date = merged_gdf['recorded_date'].max()

# Divide the dates into intervals
num_intervals = 10
date_intervals = pd.date_range(start=min_date, end=max_date, periods=num_intervals + 1)

# Select properties from each interval
selected_indices = []
for i in range(num_intervals):
    interval_start = date_intervals[i]
    interval_end = date_intervals[i + 1]
    interval_properties = merged_gdf[(merged_gdf['recorded_date'] >= interval_start) & (merged_gdf['recorded_date'] < interval_end)]
    num_properties_per_interval = len(interval_properties) // num_intervals
    selected_indices.extend(interval_properties.index.to_series().sample(n=num_properties_per_interval, random_state=42))

# Shuffle the selected indices
selected_indices = pd.Series(selected_indices).sample(frac=1, random_state=42)

# Select the corresponding rows from merged_gdf and parcels_gdf
selected_properties = merged_gdf.loc[selected_indices, ['recorded_date', 'max_dollar_value']].reset_index(drop=True)
selected_address_components = parcels_gdf.loc[selected_indices, ['ST_NUM', 'ST_NAME', 'CITY']].reset_index(drop=True)

# Combine the selected properties and address components
selected_properties = pd.concat([selected_properties, selected_address_components], axis=1)

# Diagnostic prints to confirm data size before fetching images
print("Number of selected properties:", len(selected_properties))
print("First 5 address components:", selected_properties[['ST_NUM', 'ST_NAME', 'CITY']].head())

# Retrieve street view images for the selected properties
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Prepare the address components for the street view retrieval
    address_components_list = list(selected_properties[['ST_NUM', 'ST_NAME', 'CITY']].itertuples(index=False, name=None))
    
    # Format each address before querying
    formatted_addresses = [format_address(*components) for components in address_components_list]

    # Diagnostic print to check for data duplication or size errors
    print("Unique entries check:", selected_properties.drop_duplicates().shape)
    
    # Create a tqdm progress bar to monitor the retrieval process
    with tqdm.tqdm(total=len(formatted_addresses)) as pbar:
        # Create a generator from the map function
        results = executor.map(get_street_view_image, formatted_addresses)
        # Convert results to a list while updating the progress bar after each completion
        selected_street_view_image_urls = []
        for result in results:
            selected_street_view_image_urls.append(result)
            pbar.update(1)  # Update the progress for each completed task

# Verify that the number of URLs matches the expected number of properties
print("Number of retrieved street view URLs:", len(selected_street_view_image_urls))

# Overlay text on the retrieved street view images
# Prepare the text overlay data (combining URL with respective dates and values)
overlay_data = zip(selected_street_view_image_urls, selected_properties['recorded_date'].dt.strftime('%Y-%m-%d'), selected_properties['max_dollar_value'])

processed_images = []
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(overlay_text, overlay_data))
    for result in results:
        if result is not None:
            processed_images.append(result)

# Select properties and create a montage based on music tempo
music_file = 'the_times_they_are_a_changin.mp3'

if not os.path.exists(music_file):
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': music_file,
    }
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(['https://www.youtube.com/watch?v=90WD_ats6eE'])

y, sr = librosa.load(music_file)
tempo, _ = librosa.beat.beat_track(y=y, sr=sr)

# Target video length in seconds
target_video_length = 180  # Adjust this value as needed

# Calculate the desired number of properties based on the target video length and tempo
duration_per_image = 60 / tempo  # Adjust duration based on tempo
num_selected_properties = int(target_video_length / duration_per_image)

# Check if any images were successfully processed
if not processed_images:
    print("No valid images were processed for video creation.")
else:
    print("Starting montage video creation...")
    clips = [mpy.ImageClip(img).set_duration(duration_per_image) for img in processed_images]
    
    # Ensure that there are clips to concatenate
    if clips:
        montage = mpy.concatenate_videoclips(clips, method='compose')
        print("Montage video creation completed")

        # Add music to the montage
        audio = mpy.AudioFileClip(music_file)
        final_video = montage.set_audio(audio)

        # Save the final video
        final_video.write_videofile('montage_video.mp4', fps=24)
    else:
        print("No clips available to create montage.")

