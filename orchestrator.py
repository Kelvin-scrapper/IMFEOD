"""
IMF IMFEOD Data Pipeline Orchestrator
Coordinates the complete data collection and processing workflow.

This orchestrator manages:
1. Data scraping from IMF website (main.py)
2. Data processing and Excel generation (map.py)
3. Error handling and logging
4. Pipeline status reporting
"""

import logging
import os
import sys
import time
from datetime import datetime
import subprocess
import argparse


class IMFPipelineOrchestrator:
    """
    Orchestrates the complete IMF IMFEOD data pipeline.
    
    Pipeline flow:
    1. Run main.py to scrape and download TSV files
    2. Run map.py to process data and generate Excel output
    3. Provide comprehensive status reporting
    """
    
    def __init__(self, headless=True, skip_scraping=False, output_dir="./"):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            headless (bool): Run browser in headless mode
            skip_scraping (bool): Skip scraping step (use existing files)
            output_dir (str): Directory for output files
        """
        self.headless = headless
        self.skip_scraping = skip_scraping
        self.output_dir = os.path.abspath(output_dir)
        
        # Set up logging
        log_format = '%(asctime)s - %(levelname)s - [ORCHESTRATOR] %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_format)
        self.logger = logging.getLogger(__name__)
        
        self.countries = ["Ireland", "Greece", "Portugal"]
        self.start_time = None
        self.scraping_success = False
        self.mapping_success = False
        
    def check_dependencies(self):
        """Check if all required dependencies are installed."""
        required_modules = [
            'undetected_chromedriver',
            'selenium', 
            'requests',
            'pandas',
            'openpyxl'
        ]
        
        self.logger.info("Checking dependencies...")
        missing_modules = []
        
        for module in required_modules:
            try:
                __import__(module)
                self.logger.debug(f"✓ {module} found")
            except ImportError:
                missing_modules.append(module)
                self.logger.error(f"✗ {module} missing")
        
        if missing_modules:
            self.logger.error("Missing required modules:")
            for module in missing_modules:
                self.logger.error(f"  - {module}")
            self.logger.error("Install with: pip install -r requirements.txt")
            return False
        
        self.logger.info("All dependencies satisfied ✓")
        return True
    
    def check_files_exist(self):
        """Check if required Python files exist."""
        required_files = ['main.py', 'map.py']
        
        self.logger.info("Checking required files...")
        missing_files = []
        
        for file in required_files:
            if os.path.exists(file):
                self.logger.debug(f"✓ {file} found")
            else:
                missing_files.append(file)
                self.logger.error(f"✗ {file} missing")
        
        if missing_files:
            self.logger.error("Missing required files:")
            for file in missing_files:
                self.logger.error(f"  - {file}")
            return False
        
        self.logger.info("All required files present ✓")
        return True
    
    def run_scraping_step(self):
        """Execute the data scraping step (main.py)."""
        if self.skip_scraping:
            self.logger.info("Skipping scraping step (--skip-scraping flag)")
            return True
        
        self.logger.info("="*60)
        self.logger.info("STEP 1: DATA SCRAPING (main.py)")
        self.logger.info("="*60)
        
        try:
            # Prepare environment for main.py
            env = os.environ.copy()
            if self.headless:
                # Modify main.py to use headless mode
                self.logger.info("Running scraper in headless mode")
            
            # Run main.py as subprocess
            self.logger.info("Starting IMF data scraper...")
            result = subprocess.run(
                [sys.executable, 'main.py'],
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            # Log scraper output
            if result.stdout:
                self.logger.info("Scraper output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Scraper warnings/errors:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")
            
            if result.returncode == 0:
                self.logger.info("✓ Scraping completed successfully")
                self.scraping_success = True
                return True
            else:
                self.logger.error(f"✗ Scraping failed with exit code {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("✗ Scraping timed out (10 minutes)")
            return False
        except Exception as e:
            self.logger.error(f"✗ Scraping failed: {str(e)}")
            return False
    
    def check_scraped_files(self):
        """Check if scraped files are present."""
        self.logger.info("Checking for scraped data files...")
        
        downloads_dir = "./downloads"
        found_files = []
        
        if os.path.exists(downloads_dir):
            for country in self.countries:
                pattern_files = [f for f in os.listdir(downloads_dir) 
                               if f.startswith(f"{country}_IMF_External_Arrangements")]
                if pattern_files:
                    latest_file = max(pattern_files, 
                                    key=lambda x: os.path.getmtime(os.path.join(downloads_dir, x)))
                    found_files.append(f"{country}: {latest_file}")
                    self.logger.info(f"  ✓ {country}: {latest_file}")
                else:
                    self.logger.warning(f"  ✗ {country}: No file found")
        
        if len(found_files) == len(self.countries):
            self.logger.info(f"✓ All {len(self.countries)} country files found")
            return True
        else:
            self.logger.warning(f"⚠ Only {len(found_files)}/{len(self.countries)} country files found")
            return len(found_files) > 0  # Proceed if at least some files exist
    
    def run_mapping_step(self):
        """Execute the data mapping step (map.py)."""
        self.logger.info("="*60)
        self.logger.info("STEP 2: DATA PROCESSING (map.py)")
        self.logger.info("="*60)
        
        try:
            # Run map.py as subprocess
            self.logger.info("Starting data processor...")
            result = subprocess.run(
                [sys.executable, 'map.py'],
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )
            
            # Log processor output
            if result.stdout:
                self.logger.info("Processor output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("Processor warnings/errors:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")
            
            if result.returncode == 0:
                self.logger.info("✓ Data processing completed successfully")
                self.mapping_success = True
                return True
            else:
                self.logger.error(f"✗ Data processing failed with exit code {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("✗ Data processing timed out (2 minutes)")
            return False
        except Exception as e:
            self.logger.error(f"✗ Data processing failed: {str(e)}")
            return False
    
    def find_output_files(self):
        """Find generated output files."""
        self.logger.info("Scanning for generated output files...")
        
        output_files = []
        
        # Look for Excel files
        for file in os.listdir('.'):
            if file.startswith('IMFEOD_DATA_') and file.endswith('.xlsx'):
                file_path = os.path.abspath(file)
                file_size = os.path.getsize(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                output_files.append({
                    'name': file,
                    'path': file_path,
                    'size': file_size,
                    'modified': mod_time
                })
        
        if output_files:
            # Sort by modification time (most recent first)
            output_files.sort(key=lambda x: x['modified'], reverse=True)
            
            self.logger.info("Generated output files:")
            for file_info in output_files:
                self.logger.info(f"  ✓ {file_info['name']}")
                self.logger.info(f"    Size: {file_info['size']:,} bytes")
                self.logger.info(f"    Modified: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        return output_files
    
    def generate_summary_report(self):
        """Generate a summary report of the pipeline execution."""
        end_time = datetime.now()
        duration = end_time - self.start_time if self.start_time else None
        
        self.logger.info("="*60)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("="*60)
        
        self.logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time else 'N/A'}")
        self.logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Duration: {str(duration).split('.')[0] if duration else 'N/A'}")
        self.logger.info("")
        
        self.logger.info("Step Results:")
        self.logger.info(f"  Data Scraping: {'✓ SUCCESS' if self.scraping_success else '✗ FAILED'}")
        self.logger.info(f"  Data Processing: {'✓ SUCCESS' if self.mapping_success else '✗ FAILED'}")
        self.logger.info("")
        
        output_files = self.find_output_files()
        if output_files:
            self.logger.info(f"Output: {output_files[0]['name']} ({output_files[0]['size']:,} bytes)")
        
        overall_success = self.scraping_success and self.mapping_success
        self.logger.info(f"Overall Result: {'✓ PIPELINE SUCCESS' if overall_success else '✗ PIPELINE FAILED'}")
        
        return overall_success
    
    def run_pipeline(self):
        """Execute the complete IMF data pipeline."""
        self.start_time = datetime.now()
        
        self.logger.info("IMF IMFEOD Data Pipeline Orchestrator")
        self.logger.info("="*60)
        
        # Pre-flight checks
        if not self.check_files_exist():
            return False
            
        if not self.check_dependencies():
            return False
        
        # Step 1: Data scraping
        if not self.run_scraping_step():
            self.logger.error("Pipeline failed at scraping step")
            self.generate_summary_report()
            return False
        
        # Check scraped files
        if not self.check_scraped_files():
            self.logger.error("Pipeline failed: insufficient scraped data")
            self.generate_summary_report()
            return False
        
        # Step 2: Data processing
        if not self.run_mapping_step():
            self.logger.error("Pipeline failed at processing step")
            self.generate_summary_report()
            return False
        
        # Generate final report
        success = self.generate_summary_report()
        
        if success:
            self.logger.info("🎉 Pipeline completed successfully!")
        else:
            self.logger.error("💥 Pipeline completed with errors")
        
        return success


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(description='IMF IMFEOD Data Pipeline Orchestrator')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--visible', action='store_true', 
                       help='Run browser in visible mode (overrides --headless)')
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip scraping step and use existing files')
    parser.add_argument('--output-dir', default='./',
                       help='Output directory for generated files')
    
    args = parser.parse_args()
    
    # Handle headless vs visible mode
    headless_mode = args.headless and not args.visible
    
    # Create and run orchestrator
    orchestrator = IMFPipelineOrchestrator(
        headless=headless_mode,
        skip_scraping=args.skip_scraping,
        output_dir=args.output_dir
    )
    
    success = orchestrator.run_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()