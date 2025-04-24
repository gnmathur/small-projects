#!/usr/bin/env python3

import os
import re
import sys
import xml.etree.ElementTree as ET
import argparse
from datetime import datetime
import unicodedata
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_filename(name):
    """Sanitize a string to be used as a filename."""
    # Replace spaces and special characters
    s = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'[-\s]+', '-', s).strip('-_')
    return s

def extract_podcast_info(file_path):
    """
    Try to extract podcast name and episode title from the file.
    Returns a tuple of (podcast_name, episode_title)
    """
    # This is a placeholder - ideally you would extract this from the file content
    # For now, we'll just use the filename as the episode title
    filename = os.path.basename(file_path)
    match = re.search(r'transcript_(\d+)', filename)

    if match:
        # Use the ID as episode title for now
        episode_id = match.group(1)
        return ("Unknown_Podcast", f"Episode_{episode_id}")
    else:
        return ("Unknown_Podcast", sanitize_filename(os.path.splitext(filename)[0]))

def extract_text_from_ttml(file_path):
    """
    Extract text from a TTML format file.

    Args:
        file_path: Path to the TTML file

    Returns:
        A tuple containing (extracted_text, podcast_name, episode_title, date)
    """
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Define namespaces
        namespaces = {
            'tt': 'http://www.w3.org/ns/ttml',
            'podcasts': 'http://podcasts.apple.com/transcript-ttml-internal',
            'ttm': 'http://www.w3.org/ns/ttml#metadata'
        }

        # Try to extract podcast name and title
        # This is a placeholder - you would need to adjust based on the actual TTML structure
        podcast_name = "Unknown_Podcast"
        episode_title = "Unknown_Episode"

        # Try to extract from metadata if available
        metadata = root.find('.//tt:metadata', namespaces)
        if metadata is not None:
            # Extract podcast name and episode title if available in metadata
            # This would need to be adjusted based on the actual TTML structure
            pass

        # Fallback to extracting from content
        # Look for title-like content in the first few paragraphs
        paragraphs = root.findall('.//tt:p', namespaces)
        if paragraphs and len(paragraphs) > 0:
            first_paragraph_text = ''.join(span.text or '' for span in paragraphs[0].findall('.//tt:span', namespaces))
            if "Welcome to" in first_paragraph_text and "Podcast" in first_paragraph_text:
                # Extract podcast name from welcome message
                match = re.search(r'Welcome to (the |our )?(.*?)( Podcast| Show)', first_paragraph_text)
                if match:
                    podcast_name = match.group(2).strip()

        # If we couldn't extract from content, use the filename
        if podcast_name == "Unknown_Podcast" or episode_title == "Unknown_Episode":
            podcast_name, episode_title = extract_podcast_info(file_path)

        # Find all span elements with text
        spans = root.findall('.//tt:span', namespaces)

        # Extract text from each span
        full_text = []
        current_sentence = []

        for span in spans:
            if span.text:
                # Add word to current sentence
                current_sentence.append(span.text)

            # Check if this is the end of a sentence
            if 'end' in span.attrib:
                sentence_text = ' '.join(current_sentence)
                full_text.append(sentence_text)
                current_sentence = []

        # Join sentences with proper spacing
        transcript_text = ' '.join(full_text)

        # Clean up extra spaces
        transcript_text = re.sub(r'\s+', ' ', transcript_text).strip()

        # Get the file modification date as a fallback date
        file_date = datetime.fromtimestamp(os.path.getmtime(file_path))
        date_str = file_date.strftime("%Y-%m-%d")

        return (transcript_text, podcast_name, episode_title, date_str)

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return (f"Error processing file: {str(e)}", "Error", "Error", datetime.now().strftime("%Y-%m-%d"))

def process_directory(input_dir, output_dir):
    """
    Process all TTML files in the given directory and extract their content.

    Args:
        input_dir: The directory containing TTML files
        output_dir: The directory where to save the extracted text
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Count processed files
    processed_count = 0
    error_count = 0

    # Get all files in the directory
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.ttml'):
                file_path = os.path.join(root, file)
                logger.info(f"Processing {file_path}")

                try:
                    # Extract text and metadata
                    transcript_text, podcast_name, episode_title, date_str = extract_text_from_ttml(file_path)

                    # Clean podcast name for directory
                    sanitized_podcast_name = sanitize_filename(podcast_name)

                    # Create podcast directory
                    podcast_dir = os.path.join(output_dir, sanitized_podcast_name)
                    os.makedirs(podcast_dir, exist_ok=True)

                    # Create output filename
                    sanitized_episode_title = sanitize_filename(episode_title)
                    output_filename = f"{sanitized_podcast_name}-{sanitized_episode_title}-{date_str}.txt"
                    output_path = os.path.join(podcast_dir, output_filename)

                    # Write extracted text to file
                    with open(output_path, 'w', encoding='utf-8') as out_file:
                        out_file.write(transcript_text)

                    logger.info(f"Saved transcript to {output_path}")
                    processed_count += 1

                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
                    error_count += 1

    logger.info(f"Processing complete. Processed {processed_count} files successfully with {error_count} errors.")
    return processed_count, error_count

def main():
    parser = argparse.ArgumentParser(description="Extract text from TTML podcast transcripts")
    parser.add_argument("--input", default="~/Library/Group Containers/243LU875E5.groups.com.apple.podcasts/Library/Cache/Assets/TTML",
                        help="Directory containing TTML files")
    parser.add_argument("--output", default="./podcast_transcripts",
                        help="Directory to save extracted transcripts")

    args = parser.parse_args()

    # Expand user path (for ~)
    input_dir = os.path.expanduser(args.input)
    output_dir = os.path.expanduser(args.output)

    if not os.path.exists(input_dir):
        logger.error(f"Input directory {input_dir} does not exist!")
        return 1

    logger.info(f"Processing TTML files from {input_dir}")
    processed, errors = process_directory(input_dir, output_dir)

    if processed == 0:
        logger.warning(f"No TTML files found in {input_dir}")
    else:
        logger.info(f"Successfully processed {processed} files with {errors} errors")
        logger.info(f"Transcripts saved to {output_dir}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
