import pytesseract
from PIL import Image
import cv2
import numpy as np

# Path to the image file
image_path = "187321184_2.png"

# Load the image
image = cv2.imread(image_path)

# Convert the image to grayscale
#gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Apply thresholding to binarize the image
#_, binary_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

# Deskew the image
#coords = np.column_stack(np.where(binary_image > 0))
#angle = cv2.minAreaRect(coords)[-1]
#if angle < -45:
#    angle = -(90 + angle)
#else:
#    angle = -angle
#(h, w) = binary_image.shape[:2]
#center = (w // 2, h // 2)
#M = cv2.getRotationMatrix2D(center, angle, 1.0)
#deskewed_image = cv2.warpAffine(binary_image, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

# Perform OCR using Tesseract
#text = pytesseract.image_to_string(deskewed_image, lang='eng', config='--psm 6')
ocr_text = pytesseract.image_to_string(image, lang='eng', config='--psm 6')

# Print the extracted text
print(ocr_text)

import re

# Define regex pattern to extract dollar amounts
pattern = r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?'

# Find all matches using regex
matches = re.findall(pattern, ocr_text)

# Convert matches to floats and find the largest value
max_dollar_value = max(float(match.replace('$', '').replace(',', '')) for match in matches)

# Print the largest dollar value found
print("Largest Dollar Value:", max_dollar_value)

