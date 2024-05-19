# Dallas Deed OCR

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
   ```

2. **Activate the environment**:
   ```bash
   conda activate deed_ocr_dallas
   ```
## Usage
1. Initialize the database:

```bash
python scripts/check_db.py
```

2. Run the main script:
```bash
python scripts/find_principal_dallas.py
```

3. Visualize the results:
```bash
python scripts/visualize_principal.py
```

## Contributing
Contributions are welcome. Please fork the repository and submit a pull request.

##About
This script collects Dallas' parcel-level mortgage data in reverse chronological order. To collect all data since 2020 would require ~$3000 in cloud compute costs or 1 month of compute time on a single computer of the specs below.

## License
This project is licensed under the GPLv3+ License - see the LICENSE file for details.

## System specs
- CPU: x86_64
- RAM: 15.46 GB
- Storage: 1006.85 GB
- OS: Linux 5.15.146.1-microsoft-standard-WSL2
