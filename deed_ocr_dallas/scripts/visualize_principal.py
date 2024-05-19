import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.animation import FFMpegWriter
import matplotlib.colors as mcolors
from sqlalchemy import create_engine
import datashader as ds
import datashader.transfer_functions as tf
from datashader.colors import colormap_select
import concurrent.futures
from datetime import datetime
import time
import dask_geopandas as dgd
import re
import tqdm
import numpy as np

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

# Establish a connection to the database
engine = create_engine('sqlite:///deed_data.db')

# Load the deed data
deeds_df = pd.read_sql_query("SELECT * FROM deeds WHERE max_dollar_value IS NOT NULL", engine)

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

# Define a colormap
norm = mcolors.Normalize(vmin=merged_gdf['max_dollar_value'].min(), vmax=merged_gdf['max_dollar_value'].max())
cmap = plt.get_cmap('viridis')

# Sort by date for animation
merged_gdf.sort_values('recorded_date', inplace=True)

# Simplify geometries for background plotting
print("Simplifying geometries...")
simplified_geometries = parcels_gdf.geometry.simplify(tolerance=1.0, preserve_topology=False)
print("Simplification done")

# Convert geometries to centroids
merged_gdf['geometry'] = merged_gdf.geometry.centroid

# Extract 'x' and 'y' coordinates from the 'geometry' column
merged_gdf['x'] = merged_gdf.geometry.x
merged_gdf['y'] = merged_gdf.geometry.y

# Assuming merged_gdf and date_range are prepared as before
cvs = ds.Canvas(plot_width=800, plot_height=600)
agg = cvs.points(merged_gdf, 'x', 'y', ds.mean('max_dollar_value'))

# Calculate the bounds of the data
x_min, y_min, x_max, y_max = merged_gdf.total_bounds

# Setup the plot
fig, ax = plt.subplots(figsize=(10, 10))
background = tf.shade(agg, cmap=cmap, how='eq_hist')
background_img = ax.imshow(background.to_pil(), extent=[x_min, x_max, y_min, y_max], origin='lower')

def create_frame(date):
    frame_data = merged_gdf[merged_gdf['recorded_date'] == date]
    img = tf.shade(cvs.points(frame_data, 'x', 'y', ds.mean('max_dollar_value')), cmap=plt.get_cmap('viridis'))
    return (img.to_pil(), date)

def update_plot(frame):
    frame_img, date = frame
    # Update the figure for new frame
    ax.clear()
    ax.imshow(background.to_pil(), extent=[x_min, x_max, y_min, y_max], origin='lower')
    ax.imshow(frame_img, extent=[x_min, x_max, y_min, y_max], origin='lower', alpha=0.7)
    ax.set_title(f"Mortgage Values as of {date}")

# Get the unique dates from the 'recorded_date' column
date_range = merged_gdf['recorded_date'].unique()

# Sort the date range
date_range = np.sort(date_range)

# Use multiprocessing to generate each frame
with concurrent.futures.ProcessPoolExecutor() as executor:
    frames = list(tqdm.tqdm(executor.map(create_frame, date_range), total=len(date_range)))

# Animation setup
ani = animation.FuncAnimation(fig, update_plot, frames=frames, interval=100, repeat=False)

# Save the animation as a video file
writer = animation.FFMpegWriter(fps=10)
ani.save("mortgage_values_animation.mp4", writer=writer)

plt.show()

#simplified_geometries.plot(ax=ax, color='grey', alpha=0.3)
## Scatter plot initialization removed if not updating the base plot every frame
#scatter_plot = None
#
## List of unique dates to create frames
#date_range = pd.date_range(start=merged_gdf['recorded_date'].min(), end=merged_gdf['recorded_date'].max(), freq='D')
#
## Initialize the tqdm progress bar
#pbar = tqdm.tqdm(total=len(date_range))
#
#def update(frame_date):
#    start_time = time.time()  # Start time tracking
#
#    for plot in scatter_plots:
#        plot.remove()
#    scatter_plots.clear()
#
#    # Filter data up to the current frame's date
#    current_data = merged_gdf[merged_gdf['recorded_date'] <= frame_date]
#
#    # If there are no transactions for a particular day, skip plotting
#    if not current_data.empty:
#        sc = ax.scatter(current_data['centroid'].x, current_data['centroid'].y, 
#                        c=current_data['max_dollar_value'], cmap=cmap, norm=norm, s=10, alpha=0.7)
#        fig.colorbar(sc, ax=ax, orientation='vertical', label='Mortgage Value ($)')
#        scatter_plots.append(sc)
#
#    ax.set_title(f"Mortgage Values as of {current_date.strftime('%Y-%m-%d')}")
#
#    # Time taken for this frame
#    elapsed_time = time.time() - start_time
#    print(f"Updated frame for {frame_date.strftime('%Y-%m-%d')}: {elapsed_time:.2f} seconds")
#
#    # Update the tqdm progress bar
#    pbar.update(1)
#    return sc,
#
## Create a writer for saving the video
#writer = FFMpegWriter(fps=15, metadata=dict(artist='Me'), bitrate=1800)
#
## Save the animation
#date_range = pd.date_range(start=merged_gdf['recorded_date'].min(), end=merged_gdf['recorded_date'].max(), freq='D')
#ani = animation.FuncAnimation(fig, update, frames=date_range)
#
## Save to file
#ani.save('mortgage_values.mp4', writer=writer)
#print("Animation saved.")
#
