import os
import shutil
import subprocess

# Define directories and files
base_dir = 'deed_ocr_dallas'
data_files = ['Parcel.cpg', 'Parcel.dbf', 'Parcel.parquet', 'Parcel.prj', 'Parcel.sbn', 'Parcel.sbx', 'Parcel.shp', 'Parcel.shp.xml', 'Parcel.shx', 'deed_data.db']
image_files = ['187321184_2.png', '204185040']
script_files = ['check_db.py', 'find_principal_dallas.py', 'find_principal_dallas2.py', 'ocr_text.py', 'montage_principal.py', 'visualize_principal.py']
result_files = ['deed_data.csv', 'mortgage_values.mp4', 'mortgage_values_animation.mp4', 'wget-log']
audio_files = ['the_times_they_are_a_changin.mp3', 'the_times_they_are_a_changin.webm']
test_files = ['test_find_principal.py', 'test_ocr_text.py']

# Create directory structure
os.makedirs(os.path.join(base_dir, 'data'), exist_ok=True)
os.makedirs(os.path.join(base_dir, 'images'), exist_ok=True)
os.makedirs(os.path.join(base_dir, 'scripts'), exist_ok=True)
os.makedirs(os.path.join(base_dir, 'results'), exist_ok=True)
os.makedirs(os.path.join(base_dir, 'tests'), exist_ok=True)
os.makedirs(os.path.join(base_dir, 'audio'), exist_ok=True)

# Move files to their respective directories
def move_files(file_list, target_dir):
    for file in file_list:
        if os.path.exists(file):
            shutil.move(file, os.path.join(target_dir, file))

move_files(data_files, os.path.join(base_dir, 'data'))
move_files(image_files, os.path.join(base_dir, 'images'))
move_files(script_files, os.path.join(base_dir, 'scripts'))
move_files(result_files, os.path.join(base_dir, 'results'))
move_files(audio_files, os.path.join(base_dir, 'audio'))
move_files(test_files, os.path.join(base_dir, 'tests'))

# Create README.md
readme_content = """# Dallas Deed OCR

## Overview
This project processes public records from Dallas to extract mortgage principal amounts using OCR (Optical Character Recognition). It includes scripts for data scraping, image processing, and visualization.

## Directory Structure
- **data/**: Contains parcel shapefiles and the main database.
- **images/**: Downloaded images used for OCR.
- **scripts/**: Python scripts for data processing.
- **results/**: Output files including CSVs and videos.
- **tests/**: Test scripts to verify functionality.
- **audio/**: Audio files included in the repository.

## Setup
To set up the environment, follow these steps:

1. **Create the conda environment**:
   ```bash
   conda env create -f environment.yml
"""

