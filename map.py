import pandas as pd
import re
from datetime import datetime
import logging
import os
import glob

class IMFDataMapper:
    """
    Maps IMF External Arrangements data from TSV files to a predefined Excel column format.
    
    This class handles the mapping between individual country TSV files 
    (Ireland, Greece, Portugal) and the consolidated Excel data structure
    used in the IMFEOD dataset. The column headers are hardcoded to ensure a
    consistent output format.
    """
    
    # Hardcoded machine-readable and descriptive headers based on the provided CSV file.
    HARDCODED_COLUMN_HEADERS = [
        '', 'IMFEOD.EXTFUNDFACILITY2010DEC16.AMOUNTAGREED.IRL.M', 'IMFEOD.EXTFUNDFACILITY2010DEC16.AMOUNTDRAWN.IRL.M', 'IMFEOD.EXTFUNDFACILITY2010DEC16.AMOUNTOUTSTANDING.IRL.M', 'IMFEOD.TOTAL.AMOUNTAGREED.IRL.M', 'IMFEOD.TOTAL.AMOUNTDRAWN.IRL.M', 'IMFEOD.TOTAL.AMOUNTOUTSTANDING.IRL.M', 'IMFEOD.EXTFUNDFACILITY2012MAR15.AMOUNTAGREED.GRC.M', 'IMFEOD.EXTFUNDFACILITY2012MAR15.AMOUNTDRAWN.GRC.M', 'IMFEOD.EXTFUNDFACILITY2012MAR15.AMOUNTOUTSTANDING.GRC.M', 'IMFEOD.STANDBYARRANGEMENT2010MAY09.AMOUNTAGREED.GRC.M', 'IMFEOD.STANDBYARRANGEMENT2010MAY09.AMOUNTDRAWN.GRC.M', 'IMFEOD.STANDBYARRANGEMENT2010MAY09.AMOUNTOUTSTANDING.GRC.M', 'IMFEOD.TOTAL.AMOUNTAGREED.GRC.M', 'IMFEOD.TOTAL.AMOUNTDRAWN.GRC.M', 'IMFEOD.TOTAL.AMOUNTOUTSTANDING.GRC.M', 'IMFEOD.EXTFUNDFACILITY2011MAY20.AMOUNTAGREED.PRT.M', 'IMFEOD.EXTFUNDFACILITY2011MAY20.AMOUNTDRAWN.PRT.M', 'IMFEOD.EXTFUNDFACILITY2011MAY20.AMOUNTOUTSTANDING.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1983OCT07.AMOUNTAGREED.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1983OCT07.AMOUNTDRAWN.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1983OCT07.AMOUNTOUTSTANDING.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1978JUN05.AMOUNTAGREED.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1978JUN05.AMOUNTDRAWN.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1978JUN05.AMOUNTOUTSTANDING.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1977APR25.AMOUNTAGREED.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1977APR25.AMOUNTDRAWN.PRT.M', 'IMFEOD.STANDBYARRANGEMENT1977APR25.AMOUNTOUTSTANDING.PRT.M', 'IMFEOD.TOTAL.AMOUNTAGREED.PRT.M', 'IMFEOD.TOTAL.AMOUNTDRAWN.PRT.M', 'IMFEOD.TOTAL.AMOUNTOUTSTANDING.PRT.M'
    ]
    
    HARDCODED_DESCRIPTIVE_HEADERS = [
        'IMF Lending Commitments, Extended Fund Facility 16 December 2010, Amount Agreed, Ireland', 'IMF Lending Commitments, Extended Fund Facility 16 December 2010, Amount Drawn, Ireland', 'IMF Lending Commitments, Extended Fund Facility 16 December 2010, Amount Outstanding, Ireland', 'IMF Lending Commitments, Total, Amount Agreed, Ireland', 'IMF Lending Commitments, Total, Amount Drawn, Ireland', 'IMF Lending Commitments, Total, Amount Outstanding, Ireland', 'IMF Lending Commitments, Extended Fund Facility 15 March 2012, Amount Agreed, Greece', 'IMF Lending Commitments, Extended Fund Facility 15 March 2012, Amount Drawn, Greece', 'IMF Lending Commitments, Extended Fund Facility 15 March 2012, Amount Outstanding, Greece', 'IMF Lending Commitments, Standby Arrangement 09 May 2010, Amount Agreed, Greece', 'IMF Lending Commitments, Standby Arrangement 09 May 2010, Amount Drawn, Greece', 'IMF Lending Commitments, Standby Arrangement 09 May 2010, Amount Outstanding, Greece', 'IMF Lending Commitments, Total, Amount Agreed, Greece', 'IMF Lending Commitments, Total, Amount Drawn, Greece', 'IMF Lending Commitments, Total, Amount Outstanding, Greece', 'IMF Lending Commitments, Extended Fund Facility 20 May 2011, Amount Agreed, Portugal', 'IMF Lending Commitments, Extended Fund Facility 20 May 2011, Amount Drawn, Portugal', 'IMF Lending Commitments, Extended Fund Facility 20 May 2011, Amount Outstanding, Portugal', 'IMF Lending Commitments, Standby Arrangement 07 October 1983, Amount Agreed, Portugal', 'IMF Lending Commitments, Standby Arrangement 07 October 1983, Amount Drawn, Portugal', 'IMF Lending Commitments, Standby Arrangement 07 October 1983, Amount Outstanding, Portugal', 'IMF Lending Commitments, Standby Arrangement 05 June 1978, Amount Agreed, Portugal', 'IMF Lending Commitments, Standby Arrangement 05 June 1978, Amount Drawn, Portugal', 'IMF Lending Commitments, Standby Arrangement 05 June 1978, Amount Outstanding, Portugal', 'IMF Lending Commitments, Standby Arrangement 25 April 1977, Amount Agreed, Portugal', 'IMF Lending Commitments, Standby Arrangement 25 April 1977, Amount Drawn, Portugal', 'IMF Lending Commitments, Standby Arrangement 25 April 1977, Amount Outstanding, Portugal', 'IMF Lending Commitments, Total, Amount Agreed, Portugal', 'IMF Lending Commitments, Total, Amount Drawn, Portugal', 'IMF Lending Commitments, Total, Amount Outstanding, Portugal'
    ]

    def __init__(self):
        self.country_codes = {
            'Ireland': 'IRL',
            'Greece': 'GRC', 
            'Portugal': 'PRT'
        }
        
        # Add flexible country name mappings for universal detection
        self.country_name_mappings = {
            'ireland': 'Ireland',
            'irish': 'Ireland',
            'republic of ireland': 'Ireland',
            'greece': 'Greece', 
            'greek': 'Greece',
            'hellenic republic': 'Greece',
            'hellas': 'Greece',
            'portugal': 'Portugal',
            'portuguese': 'Portugal',
            'portuguese republic': 'Portugal'
        }
        
        self.facility_type_mapping = {
            'Extended Fund Facility': 'EXTFUNDFACILITY',
            'Standby Arrangement': 'STANDBYARRANGEMENT'
        }
        
        self.amount_types = ['AMOUNTAGREED', 'AMOUNTDRAWN', 'AMOUNTOUTSTANDING']
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def find_country_files(self, search_directory='.'):
        """
        Recursively find TSV/Excel files for each country in the specified directory and all subdirectories.
        Works with files downloaded by main.py scraper.
        
        Args:
            search_directory (str): Root directory to search for files (default: current directory)
            
        Returns:
            dict: Dictionary mapping country names to their file paths
        """
        country_files = {}
        
        self.logger.info(f"Recursively scanning {os.path.abspath(search_directory)} for country files...")
        self.logger.info("Looking for files created by main.py scraper...")
        
        for country in self.country_codes.keys():
            # Search for files that start with the country name (case variations)
            # Compatible with main.py naming: Country_IMF_External_Arrangements_DATE
            patterns = [
                f"{country}_*",
                f"{country.lower()}_*",
                f"{country.upper()}_*"
            ]
            
            found_files = []
            
            # Walk through all directories and subdirectories
            for root, dirs, files in os.walk(search_directory):
                self.logger.debug(f"Scanning directory: {root}")
                
                for filename in files:
                    # Check if file matches any country pattern
                    for pattern in patterns:
                        if filename.startswith(pattern.replace('_*', '_')):
                            full_path = os.path.join(root, filename)
                            found_files.append(full_path)
                            self.logger.debug(f"Found potential {country} file: {full_path}")
                            break
            
            if found_files:
                # Take the most recent file if multiple found (latest scrape from main.py)
                latest_file = max(found_files, key=os.path.getmtime)
                country_files[country] = latest_file
                relative_path = os.path.relpath(latest_file, search_directory)
                self.logger.info(f"Found file for {country}: {relative_path}")
                
                # Log all found files for this country if more than one
                if len(found_files) > 1:
                    self.logger.info(f"  Multiple {country} files found, using most recent:")
                    for f in sorted(found_files, key=os.path.getmtime, reverse=True):
                        rel_path = os.path.relpath(f, search_directory)
                        timestamp = datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d %H:%M:%S')
                        marker = " (SELECTED)" if f == latest_file else ""
                        self.logger.info(f"    {rel_path} - {timestamp}{marker}")
            else:
                self.logger.warning(f"No file found for {country} in {search_directory} or subdirectories")
                self.logger.info(f"  Tip: Run main.py first to download {country} data")
        
        return country_files
    
    def scan_directory_structure(self, search_directory='.'):
        """
        Display the directory structure for debugging purposes.
        
        Args:
            search_directory (str): Root directory to scan
        """
        self.logger.info(f"Directory structure of {os.path.abspath(search_directory)}:")
        
        for root, dirs, files in os.walk(search_directory):
            level = root.replace(search_directory, '').count(os.sep)
            indent = ' ' * 2 * level
            self.logger.info(f"{indent}{os.path.basename(root)}/")
            
            sub_indent = ' ' * 2 * (level + 1)
            for file in files:
                self.logger.info(f"{sub_indent}{file}")
                
            # Limit depth to avoid too much output
            if level > 3:
                dirs[:] = []  # Don't recurse deeper
    
    def parse_date_to_code(self, date_str):
        """
        Convert date string like 'Dec 16, 2010' to format like '2010DEC16'
        """
        try:
            dt = datetime.strptime(date_str, '%b %d, %Y')
            return f"{dt.year}{dt.strftime('%b').upper()}{dt.day:02d}"
        except Exception as e:
            self.logger.warning(f"Could not parse date '{date_str}': {e}")
            return date_str.replace(' ', '').replace(',', '').upper()
    
    def generate_column_header(self, country, facility_type, date_str, amount_type):
        """
        Generate Excel column header based on TSV data.
        """
        country_code = self.country_codes.get(country, country.upper()[:3])
        facility_code = self.facility_type_mapping.get(facility_type, facility_type.upper().replace(' ', ''))
        date_code = self.parse_date_to_code(date_str)
        
        return f"IMFEOD.{facility_code}{date_code}.{amount_type}.{country_code}.M"
    
    def detect_columns(self, header_line):
        """
        Dynamically detect column positions by header names.
        
        Args:
            header_line (str): Tab-separated header line
            
        Returns:
            dict: Mapping of column names to indices
        """
        headers = header_line.strip().split('\t')
        column_map = {}
        
        # Define flexible column patterns to match
        column_patterns = {
            'facility_type': ['facility', 'type', 'arrangement type'],
            'arrangement_date': ['date', 'arrangement date', 'effective date', 'approval date'],
            'amount_agreed': ['agreed', 'amount agreed', 'committed', 'commitment'],
            'amount_drawn': ['drawn', 'amount drawn', 'disbursed', 'disbursement'],
            'amount_outstanding': ['outstanding', 'amount outstanding', 'balance', 'remaining']
        }
        
        # Match headers to columns (case-insensitive, flexible matching)
        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            for field_name, patterns in column_patterns.items():
                for pattern in patterns:
                    if pattern in header_lower:
                        column_map[field_name] = i
                        self.logger.debug(f"Mapped '{header}' (col {i}) to {field_name}")
                        break
                if field_name in column_map:
                    break
        
        # Log detected mappings
        self.logger.info(f"Column mappings detected: {column_map}")
        
        return column_map
    
    def find_country_name(self, lines):
        """
        Dynamically find country name using multiple patterns.
        
        Args:
            lines (list): File lines
            
        Returns:
            str: Country name or None
        """
        country_patterns = [
            r'([A-Za-z\s]+):\s*History of Lending Commitments',
            r'Country:\s*([A-Za-z\s]+)',
            r'^([A-Za-z\s]+)\s*-\s*IMF',
            r'IMF.*Commitments.*?([A-Za-z\s]+)$'
        ]
        
        for line in lines[:20]:  # Check first 20 lines
            for pattern in country_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    country_name = match.group(1).strip()
                    self.logger.info(f"Country detected: {country_name}")
                    return country_name
        
        self.logger.warning("Could not detect country name from file")
        return None
    
    def find_data_start(self, lines):
        """
        Dynamically find where data starts using multiple patterns.
        
        Args:
            lines (list): File lines
            
        Returns:
            tuple: (data_start_index, column_mappings) or (None, None)
        """
        header_patterns = [
            r'facility',
            r'arrangement',
            r'type',
            r'date.*amount',
            r'agreed.*drawn.*outstanding'
        ]
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            # Check if this looks like a header line
            if any(pattern in line_lower for pattern in header_patterns):
                if '\t' in line:  # Must be tab-separated
                    self.logger.info(f"Data header found at line {i+1}: {line.strip()}")
                    column_mappings = self.detect_columns(line)
                    return i + 1, column_mappings
        
        self.logger.warning("Could not find data header line")
        return None, None

    def parse_tsv_file(self, file_path):
        """
        Parse TSV file with dynamic structure detection.
        Now works regardless of column order or file structure changes.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            
            self.logger.info(f"Parsing file with dynamic detection: {file_path}")
            
            # Dynamically detect country name
            country_name = self.find_country_name(lines)
            
            # Dynamically find data start and column mappings
            data_start_idx, column_map = self.find_data_start(lines)
            
            if data_start_idx is None or not column_map:
                raise ValueError("Could not detect data structure in TSV file")
            
            # Check that we have the essential columns
            required_fields = ['facility_type', 'amount_agreed', 'amount_drawn', 'amount_outstanding']
            missing_fields = [f for f in required_fields if f not in column_map]
            if missing_fields:
                self.logger.warning(f"Missing columns: {missing_fields} - will use 0 values")
            
            # Parse data using dynamic column positions
            facilities = []
            for line_num, line in enumerate(lines[data_start_idx:], data_start_idx + 1):
                parts = line.strip().split('\t')
                if len(parts) < 2:  # Skip empty or too-short lines
                    continue
                
                # Skip total rows (flexible detection)
                first_cell = parts[0].strip().lower()
                if any(word in first_cell for word in ['total', 'sum', 'grand', 'overall']):
                    continue
                
                # Skip empty facility names
                if not parts[0].strip():
                    continue
                
                try:
                    # Extract data using dynamic column positions
                    facility_data = {}
                    
                    # Always get facility type (required)
                    facility_data['facility_type'] = parts[column_map.get('facility_type', 0)].strip()
                    
                    # Get arrangement date (with fallback)
                    if 'arrangement_date' in column_map:
                        facility_data['arrangement_date'] = parts[column_map['arrangement_date']].strip()
                    else:
                        facility_data['arrangement_date'] = ''
                    
                    # Get amounts with safe extraction
                    for amount_field in ['amount_agreed', 'amount_drawn', 'amount_outstanding']:
                        if amount_field in column_map and column_map[amount_field] < len(parts):
                            facility_data[amount_field] = self.clean_amount(parts[column_map[amount_field]])
                        else:
                            facility_data[amount_field] = 0.0
                            
                    facilities.append(facility_data)
                    
                except (IndexError, ValueError) as e:
                    self.logger.warning(f"Skipping malformed line {line_num}: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(facilities)} facilities")
            
            return {
                'country': country_name,
                'facilities': facilities
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing TSV file {file_path}: {e}")
            raise
    
    def clean_amount(self, amount_str):
        """
        Clean amount string by removing commas and converting to float.
        """
        return float(amount_str.replace(',', '').strip() or 0)
    
    def generate_mapping_from_tsv(self, tsv_data):
        """
        Generate column mappings from parsed TSV data.
        """
        mappings = {}
        country = tsv_data['country']
        
        # Calculate totals
        total_agreed = sum(f['amount_agreed'] for f in tsv_data['facilities'])
        total_drawn = sum(f['amount_drawn'] for f in tsv_data['facilities'])
        total_outstanding = sum(f['amount_outstanding'] for f in tsv_data['facilities'])
        
        # Add total columns
        mappings[f"IMFEOD.TOTAL.AMOUNTAGREED.{self.country_codes[country]}.M"] = total_agreed
        mappings[f"IMFEOD.TOTAL.AMOUNTDRAWN.{self.country_codes[country]}.M"] = total_drawn
        mappings[f"IMFEOD.TOTAL.AMOUNTOUTSTANDING.{self.country_codes[country]}.M"] = total_outstanding
        
        # Add individual facility columns
        for facility in tsv_data['facilities']:
            for amount_type in self.amount_types:
                header = self.generate_column_header(
                    country, 
                    facility['facility_type'],
                    facility['arrangement_date'],
                    amount_type
                )
                
                if 'AGREED' in amount_type:
                    mappings[header] = facility['amount_agreed']
                elif 'DRAWN' in amount_type:
                    mappings[header] = facility['amount_drawn']
                elif 'OUTSTANDING' in amount_type:
                    mappings[header] = facility['amount_outstanding']
        
        return mappings

    def extract_date_from_filename(self, filename):
        """
        Extract date from filename and return in YYYY-MM format.
        
        Args:
            filename (str): Filename to extract date from
            
        Returns:
            str: Date in YYYY-MM format, or "unknown" if not found
        """
        # Extract just the filename without path
        base_filename = os.path.basename(filename)
        
        # Common date patterns in filenames
        date_patterns = [
            r'(\d{4}-\d{2}-\d{2})',      # "2025-08-31"
            r'(\d{4}\d{2}\d{2})',        # "20250831"
            r'(\d{2}-\d{2}-\d{4})',      # "31-08-2025"
            r'(\d{2}/\d{2}/\d{4})',      # "31/08/2025"
            r'(\d{4}_\d{2}_\d{2})',      # "2025_08_31"
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, base_filename)
            if matches:
                date_str = matches[0]
                self.logger.debug(f"Found date in filename {base_filename}: {date_str}")
                
                # Parse different formats to YYYY-MM
                if re.match(r'\d{4}-\d{2}-\d{2}', date_str):  # 2025-08-31
                    return date_str[:7]  # Return YYYY-MM
                elif re.match(r'\d{8}', date_str):  # 20250831
                    return f"{date_str[:4]}-{date_str[4:6]}"
                elif re.match(r'\d{2}-\d{2}-\d{4}', date_str):  # 31-08-2025
                    parts = date_str.split('-')
                    return f"{parts[2]}-{parts[1]}"
                elif re.match(r'\d{2}/\d{2}/\d{4}', date_str):  # 31/08/2025
                    parts = date_str.split('/')
                    return f"{parts[2]}-{parts[1]}"
                elif re.match(r'\d{4}_\d{2}_\d{2}', date_str):  # 2025_08_31
                    return date_str[:7].replace('_', '-')
        
        self.logger.warning(f"No date pattern found in filename: {base_filename}")
        return "unknown"

    def process_all_countries(self, tsv_files):
        """
        Process all country TSV files and generate a complete data mapping.
        Also extracts dates from filenames for universal mapping.
        """
        all_mappings = {}
        extracted_dates = {}
        
        for country, file_path in tsv_files.items():
            self.logger.info(f"Processing {country} data from {file_path}")
            try:
                # Extract date from filename
                file_date = self.extract_date_from_filename(file_path)
                extracted_dates[country] = file_date
                self.logger.info(f"Extracted date for {country}: {file_date}")
                
                # Process the TSV data
                tsv_data = self.parse_tsv_file(file_path)
                country_mappings = self.generate_mapping_from_tsv(tsv_data)
                all_mappings.update(country_mappings)
            except Exception as e:
                self.logger.error(f"Failed to process {country}: {e}")
        
        # Store extracted dates for later use
        self.extracted_dates = extracted_dates
        return all_mappings

    def create_excel_row_from_hardcoded_headers(self, date_value, mappings):
        """
        Create a data row for Excel using the hardcoded header order.
        Returns only the data values (without date) since date is handled separately.
        """
        row = []
        
        # Iterate through the hardcoded headers (skipping the first empty one for the date column)
        # and populate the row with mapped values, defaulting to 0.
        for header in self.HARDCODED_COLUMN_HEADERS[1:]:
            row.append(mappings.get(header, 0))
        
        return row
    
    def export_to_excel(self, mappings, output_file, date_value=None):
        """
        Export mappings to an Excel file using the hardcoded header structure.
        Creates a structure matching the target:
        - Row 1: Machine-readable headers
        - Row 2: Descriptive headers  
        - Row 3: Data values
        
        Args:
            mappings: Data mappings from processed files
            output_file: Output Excel file path
            date_value: Date value extracted from filenames (YYYY-MM format)
        """
        if date_value is None:
            raise ValueError("date_value must be provided - extract from processed filenames")
        
        self.logger.info(f"Creating Excel export with dynamic date: {date_value}")
        
        # Create the data row based on the fixed header structure
        data_row = self.create_excel_row_from_hardcoded_headers(date_value, mappings)
        
        self.logger.info(f"Data row length: {len(data_row)}")
        self.logger.info(f"Hardcoded headers length: {len(self.HARDCODED_COLUMN_HEADERS)}")
        self.logger.info(f"Descriptive headers length: {len(self.HARDCODED_DESCRIPTIVE_HEADERS)}")
        
        # Ensure all arrays have the same length
        if len(data_row) != len(self.HARDCODED_DESCRIPTIVE_HEADERS):
            self.logger.warning(f"Length mismatch: data_row={len(data_row)}, descriptive_headers={len(self.HARDCODED_DESCRIPTIVE_HEADERS)}")
            # Pad or trim to match
            if len(data_row) < len(self.HARDCODED_DESCRIPTIVE_HEADERS):
                data_row.extend([0] * (len(self.HARDCODED_DESCRIPTIVE_HEADERS) - len(data_row)))
            else:
                data_row = data_row[:len(self.HARDCODED_DESCRIPTIVE_HEADERS)]
        
        # Create DataFrame with three rows to match target structure:
        # Row 1: Machine-readable headers
        # Row 2: Descriptive headers
        # Row 3: Data values with date
        
        # Prepare the full data row (date + values)
        full_data_row = [date_value] + data_row
        
        # Create DataFrame with exact structure
        df_data = []
        
        # Row 1: Machine-readable headers (first row)
        df_data.append(self.HARDCODED_COLUMN_HEADERS)
        
        # Row 2: Descriptive headers (second row) 
        # Add empty string for date column to match
        descriptive_row = [''] + self.HARDCODED_DESCRIPTIVE_HEADERS
        df_data.append(descriptive_row)
        
        # Row 3: Data values (third row)
        df_data.append(full_data_row)
        
        # Create DataFrame without column headers (we'll include them in the data)
        df = pd.DataFrame(df_data)
        
        # Export to Excel without default headers since our data contains the headers
        df.to_excel(output_file, index=False, header=False)
        self.logger.info(f"Exported data to {output_file} with proper two-header structure.")
        
        return df

def main():
    """
    Main function to run the IMF data processing pipeline.
    Works with files downloaded by main.py scraper.
    
    Pipeline:
    1. Run main.py to scrape and download TSV files
    2. Run map.py to process and map data to Excel
    """
    mapper = IMFDataMapper()
    
    mapper.logger.info("IMF Data Mapper - Processing files from main.py scraper")
    mapper.logger.info("Pipeline: main.py (download) → map.py (process) → Excel output")
    
    # Optionally show directory structure for debugging
    # mapper.scan_directory_structure('.')
    
    # Automatically find country files recursively in current directory and all subdirectories
    mapper.logger.info("Recursively searching for country data files...")
    tsv_files = mapper.find_country_files('.')
    
    if not tsv_files:
        mapper.logger.error("No country files found!")
        mapper.logger.error("Please run main.py first to download country data files")
        mapper.logger.info("Expected files: Ireland_IMF_External_Arrangements_YYYY-MM-DD")
        mapper.logger.info("                Greece_IMF_External_Arrangements_YYYY-MM-DD") 
        mapper.logger.info("                Portugal_IMF_External_Arrangements_YYYY-MM-DD")
        return
    
    mapper.logger.info(f"Found {len(tsv_files)} country files to process")

    # Process all countries and generate the data mappings
    all_mappings = mapper.process_all_countries(tsv_files)
    
    # Get the most common date from processed files  
    date_value = None
    if hasattr(mapper, 'extracted_dates') and mapper.extracted_dates:
        # Use the first valid date found, or find most common date
        valid_dates = [d for d in mapper.extracted_dates.values() if d != "unknown"]
        if valid_dates:
            # Use the most recent date (for universal processing of any month's data)
            date_value = max(valid_dates)
            mapper.logger.info(f"Dynamically extracted date for Excel output: {date_value}")
            
            # Show all extracted dates for transparency
            mapper.logger.info("Dates extracted from all processed files:")
            for country, date in mapper.extracted_dates.items():
                mapper.logger.info(f"  {country}: {date}")
        else:
            mapper.logger.error("No valid dates could be extracted from filenames!")
            return
    else:
        mapper.logger.error("No dates were extracted from processed files!")
        return
    
    # Generate output filename with the dynamically extracted date
    date_for_filename = date_value.replace('-', '')
    output_filename = f'IMFEOD_DATA_{date_for_filename}_OUTPUT.xlsx'
    mapper.logger.info(f"Output will be saved as: {output_filename}")
    
    # Export with the extracted date (fully universal - works for any month/year)
    mapper.export_to_excel(all_mappings, output_filename, date_value)
    
    print(f"\nProcessing complete. Output file created: {output_filename}")
    print(f"Processed files: {list(tsv_files.values())}")

if __name__ == "__main__":
    main()