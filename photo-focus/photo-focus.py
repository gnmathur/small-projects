#!/usr/bin/env python3
"""
Out-of-Focus Photo Detector and Remover
How to Use
This script will scan all images in a directory, analyze their focus quality, and optionally delete the blurry ones.
Requirements
Install the required packages:
Copypip install opencv-python numpy tqdm
Basic Usage

To analyze images without deleting them:

Copypython focus_detector.py /path/to/photos

To analyze and delete out-of-focus images:

Copypython focus_detector.py /path/to/photos --delete

For safer operation, use backup mode instead of permanent deletion:
python focus_detector.py /path/to/photos --delete --backup-dir /path/to/backup
Advanced Options

Adjust the threshold value to control sensitivity (default: 100)
Copypython focus_detector.py /path/to/photos --threshold 80

Lower threshold = more images marked as blurry
Higher threshold = stricter focus requirements


Use preview mode to see each blurry image before deletion:
Copypython focus_detector.py /path/to/photos --delete --preview
Press any key to proceed with deletion, or 's' to skip deleting that image

Use verbose mode for more detailed output:
python focus_detector.py /path/to/photos --verbose


The script uses the Laplacian variance method, which calculates the variance of the Laplacian of the image - a common
technique for measuring focus. Higher variance indicates sharper edges and better focus.

"""

import os
import cv2
import numpy as np
import argparse
import signal
import sys
from pathlib import Path
from tqdm import tqdm

def variance_of_laplacian(image):
    """
    Compute the Laplacian variance of an image.

    A higher variance indicates a more in-focus image, while a lower
    variance indicates a blurry/out-of-focus image.

    Args:
        image: Grayscale image to analyze

    Returns:
        Variance of the Laplacian
    """
    return cv2.Laplacian(image, cv2.CV_64F).var()

def is_focused(image_path, threshold):
    """
    Determine if an image is in focus based on Laplacian variance.

    Args:
        image_path: Path to the image file
        threshold: Laplacian variance threshold to consider an image in focus

    Returns:
        Boolean indicating if the image is in focus, None if the image couldn't be read
    """
    try:
        # Read the image
        image = cv2.imread(str(image_path))
        if image is None:
            print(f"Warning: Could not read {image_path}")
            return None

        # Convert to grayscale
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # Calculate the variance of the Laplacian
        focus_measure = variance_of_laplacian(gray)

        # Return True if the image is in focus
        return focus_measure >= threshold

    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return None

def analyze_directory(directory, threshold, delete=False, backup_dir=None, verbose=False):
    """
    Analyze all images in a directory and optionally delete out-of-focus ones.

    Args:
        directory: Directory containing images to analyze
        threshold: Laplacian variance threshold for focus detection
        delete: Whether to delete out-of-focus images
        backup_dir: Directory to move deleted images to (instead of permanent deletion)
        verbose: Whether to print detailed information

    Returns:
        Dictionary with analysis results
    """
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif'}
    path = Path(directory)

    # Create backup directory if specified
    if delete and backup_dir:
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True, parents=True)

    # Initialize counters
    stats = {
        'total': 0,
        'focused': 0,
        'blurry': 0,
        'errors': 0,
        'deleted': 0
    }

    # Get list of image files
    image_files = [f for f in path.glob('**/*') if f.is_file() and f.suffix.lower() in image_extensions]

    print(f"Found {len(image_files)} images to analyze")

    # Process each image with proper CTRL-C handling
    try:
        for image_path in tqdm(image_files, desc="Analyzing images"):
            stats['total'] += 1

            focus_result = is_focused(image_path, threshold)

            if focus_result is None:
                stats['errors'] += 1
                continue

            if focus_result:
                stats['focused'] += 1
                if verbose:
                    print(f"✅ {image_path} is in focus")
            else:
                stats['blurry'] += 1
                if verbose:
                    print(f"❌ {image_path} is out of focus")

                # Get focus measure for reporting
                if verbose:
                    image = cv2.imread(str(image_path))
                    if image is not None:
                        focus_measure = variance_of_laplacian(cv2.cvtColor(image, cv2.COLOR_BGR2GRAY))
                        print(f"  Focus measure: {focus_measure:.2f}")

                # Delete or backup out-of-focus image
                if delete:
                    if backup_dir:
                        # Move to backup directory
                        backup_file = backup_path / image_path.name
                        # Handle filename conflicts
                        if backup_file.exists():
                            i = 1
                            while True:
                                new_name = f"{backup_file.stem}_{i}{backup_file.suffix}"
                                backup_file = backup_path / new_name
                                if not backup_file.exists():
                                    break
                                i += 1

                        image_path.rename(backup_file)
                        if verbose:
                            print(f"Moved {image_path} to {backup_file}")
                    else:
                        # Permanently delete
                        image_path.unlink()
                        if verbose:
                            print(f"Deleted {image_path}")

                    stats['deleted'] += 1
    except KeyboardInterrupt:
        print("\nAnalysis interrupted!")

    return stats

def main():
    # Handle CTRL-C gracefully at the top level
    import signal

    def signal_handler(sig, frame):
        print("\nProgram terminated by user (CTRL-C)")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="Detect and delete out-of-focus photos")
    parser.add_argument("directory", help="Directory containing images to analyze")
    parser.add_argument("--threshold", type=float, default=100.0,
                        help="Laplacian variance threshold (lower values = more deletions)")
    parser.add_argument("--delete", action="store_true",
                        help="Delete out-of-focus images")
    # Preview option removed
    parser.add_argument("--backup-dir", type=str,
                        help="Move deleted images to this directory instead of permanent deletion")
    parser.add_argument("--verbose", action="store_true",
                        help="Print detailed information")

    args = parser.parse_args()

    print(f"Analyzing directory: {args.directory}")
    print(f"Focus threshold: {args.threshold}")
    print(f"Delete mode: {'ON' if args.delete else 'OFF'}")

    if args.delete and args.backup_dir:
        print(f"Backup directory: {args.backup_dir}")

    try:
        stats = analyze_directory(
            args.directory,
            args.threshold,
            delete=args.delete,
            backup_dir=args.backup_dir,
            verbose=args.verbose
        )
    except KeyboardInterrupt:
        print("\nProcess interrupted by user (CTRL-C)")
        return

    # Print results
    print("\nResults:")
    print(f"Total images: {stats['total']}")
    print(f"In-focus images: {stats['focused']}")
    print(f"Out-of-focus images: {stats['blurry']}")
    if args.delete:
        print(f"Deleted images: {stats['deleted']}")
    if stats['errors'] > 0:
        print(f"Errors: {stats['errors']}")

if __name__ == "__main__":
    main()
