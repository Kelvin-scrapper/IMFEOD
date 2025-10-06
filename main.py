"""
IMF IMFEOD Data Scraper
Automates the process of downloading TSV files for Ireland, Greece, and Portugal
from the IMF External Arrangements database.
"""

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import os
from datetime import datetime
import logging
import subprocess
import re
import platform
import requests
import shutil
import zipfile

# Configuration
HEADLESS_MODE = True  # Set to True for headless mode, False for visible browser

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IMFScraper:
    def __init__(self, download_dir=None, headless=None):
        """
        Initialize the IMF scraper
        
        Args:
            download_dir (str): Directory to save downloaded files. 
                               If None, uses current directory + 'downloads'
            headless (bool): Whether to run browser in headless mode.
                           If None, uses global HEADLESS_MODE setting
        """
        self.url = "http://www.imf.org/external/np/fin/tad/extarr1.aspx"
        self.countries = {
            "Ireland": "470",
            "Greece": "360", 
            "Portugal": "810"
        }
        
        # Set headless mode - use parameter if provided, otherwise use global setting
        self.headless = headless if headless is not None else HEADLESS_MODE
        
        # Set up download directory
        if download_dir is None:
            self.download_dir = os.path.join(os.getcwd(), "imf_downloads")
        else:
            self.download_dir = download_dir
            
        # Convert to absolute path
        self.download_dir = os.path.abspath(self.download_dir)
            
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)
        
        self.driver = None
        
    def detect_chrome_version(self):
        """
        Detect the installed Chrome version automatically

        Returns:
            str: Chrome version string or None if not found
        """
        try:
            system = platform.system()
            logger.info(f"Detecting Chrome version on {system}")

            if system == "Windows":
                # Method 1: Try registry
                try:
                    import winreg
                    key_path = r"Software\Google\Chrome\BLBeacon"
                    for hive in [winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE]:
                        try:
                            key = winreg.OpenKey(hive, key_path)
                            version, _ = winreg.QueryValueEx(key, "version")
                            winreg.CloseKey(key)
                            logger.info(f"Detected Chrome version from registry: {version}")
                            return version
                        except:
                            continue
                except Exception as e:
                    logger.warning(f"Registry method failed: {str(e)}")

                # Method 2: Check version folders in Chrome Application directory
                paths = [
                    r"C:\Program Files\Google\Chrome\Application",
                    r"C:\Program Files (x86)\Google\Chrome\Application",
                    r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\Application"
                ]

                for path in paths:
                    expanded_path = os.path.expandvars(path)
                    if os.path.exists(expanded_path):
                        for item in os.listdir(expanded_path):
                            if re.match(r'^\d+\.\d+\.\d+\.\d+$', item):
                                logger.info(f"Detected Chrome version from folder: {item}")
                                return item
                        break

            elif system == "Darwin":  # macOS
                try:
                    result = subprocess.run(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--version"],
                                          capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', result.stdout)
                        if version_match:
                            version = version_match.group(1)
                            logger.info(f"Detected Chrome version: {version}")
                            return version
                except FileNotFoundError:
                    pass

            elif system == "Linux":
                try:
                    # Try common Linux Chrome commands
                    commands = ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium"]
                    for cmd in commands:
                        try:
                            result = subprocess.run([cmd, "--version"],
                                                  capture_output=True, text=True, timeout=10)
                            if result.returncode == 0:
                                version_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', result.stdout)
                                if version_match:
                                    version = version_match.group(1)
                                    logger.info(f"Detected Chrome version: {version}")
                                    return version
                        except FileNotFoundError:
                            continue
                except Exception:
                    pass

            logger.warning("Could not detect Chrome version automatically")
            return None

        except Exception as e:
            logger.warning(f"Error detecting Chrome version: {str(e)}")
            return None

    def download_chromedriver(self, version):
        """
        Download the correct ChromeDriver version for the detected Chrome version

        Args:
            version (str): Chrome version string (e.g., "140.0.7339.208")

        Returns:
            str: Path to the downloaded ChromeDriver executable, or None if failed
        """
        try:
            major_version = version.split('.')[0]
            logger.info(f"Downloading ChromeDriver for Chrome {major_version}")

            # ChromeDriver download URL pattern
            system = platform.system()
            if system == "Windows":
                platform_suffix = "win64"
                driver_name = "chromedriver.exe"
            elif system == "Darwin":
                platform_suffix = "mac-x64"
                driver_name = "chromedriver"
            elif system == "Linux":
                platform_suffix = "linux64"
                driver_name = "chromedriver"
            else:
                logger.error(f"Unsupported platform: {system}")
                return None

            # Try to get the latest patch version for the major version
            # ChromeDriver JSON endpoint
            try:
                # First, try to get the latest version for this major version
                json_url = f"https://googlechromelabs.github.io/chrome-for-testing/latest-patch-versions-per-build-with-downloads.json"
                response = requests.get(json_url, timeout=10)
                response.raise_for_status()
                data = response.json()

                if major_version in data.get('builds', {}):
                    build_info = data['builds'][major_version]
                    full_version = build_info['version']

                    # Find the chromedriver download URL
                    downloads = build_info.get('downloads', {}).get('chromedriver', [])
                    download_url = None
                    for download in downloads:
                        if download.get('platform') == platform_suffix:
                            download_url = download.get('url')
                            break

                    if not download_url:
                        logger.warning(f"No download URL found for platform {platform_suffix}")
                        return None

                    logger.info(f"Found ChromeDriver version {full_version}")
                    logger.info(f"Download URL: {download_url}")

                    # Download the zip file
                    zip_response = requests.get(download_url, timeout=60)
                    zip_response.raise_for_status()

                    # Create a temp directory for extraction
                    temp_dir = os.path.join(os.getcwd(), "temp_chromedriver")
                    os.makedirs(temp_dir, exist_ok=True)

                    zip_path = os.path.join(temp_dir, "chromedriver.zip")
                    with open(zip_path, 'wb') as f:
                        f.write(zip_response.content)

                    # Extract the zip
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)

                    # Find the chromedriver executable
                    driver_path = None
                    for root, dirs, files in os.walk(temp_dir):
                        if driver_name in files:
                            driver_path = os.path.join(root, driver_name)
                            break

                    if driver_path and os.path.exists(driver_path):
                        # Make it executable on Unix systems
                        if system in ["Darwin", "Linux"]:
                            os.chmod(driver_path, 0o755)

                        logger.info(f"ChromeDriver downloaded successfully: {driver_path}")
                        return driver_path
                    else:
                        logger.error("Could not find chromedriver in extracted files")
                        return None
                else:
                    logger.warning(f"No ChromeDriver build found for Chrome {major_version}")
                    return None

            except Exception as e:
                logger.error(f"Error downloading ChromeDriver: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"Error in download_chromedriver: {str(e)}")
            return None
    
    def set_download_directory(self):
        """Set the download directory using Chrome DevTools Protocol"""
        try:
            # Enable downloads and set download directory
            self.driver.execute_cdp_cmd('Page.setDownloadBehavior', {
                'behavior': 'allow',
                'downloadPath': self.download_dir
            })
            logger.info(f"Download directory set to: {self.download_dir}")
        except Exception as e:
            logger.warning(f"Could not set download directory via CDP: {str(e)}")
        
    def setup_driver(self):
        """Set up the undetected Chrome driver with download preferences and auto version detection"""

        # Detect Chrome version
        chrome_version = self.detect_chrome_version()
        driver_executable_path = None
        major_version = None

        if chrome_version:
            try:
                major_version = int(chrome_version.split('.')[0])
                logger.info(f"Detected Chrome major version: {major_version}")

                # Download the correct ChromeDriver version
                driver_executable_path = self.download_chromedriver(chrome_version)
                if driver_executable_path:
                    logger.info(f"Using custom ChromeDriver: {driver_executable_path}")
            except Exception as e:
                logger.warning(f"Could not download matching ChromeDriver: {str(e)}")

        # Try multiple initialization strategies
        strategies = []

        # Strategy 1: Use downloaded chromedriver if available
        if driver_executable_path:
            strategies.append(('custom_driver', driver_executable_path))

        # Strategy 2: Use detected version
        if major_version:
            strategies.append(('detected_version', major_version))

        # Strategy 3: Try without version (auto-detect)
        strategies.append(('auto', None))

        # Strategy 4: Use subprocess mode
        strategies.append(('subprocess', None))

        for strategy_name, param in strategies:
            try:
                logger.info(f"Trying initialization strategy: {strategy_name}")

                options = uc.ChromeOptions()

                # Set download preferences
                prefs = {
                    "download.default_directory": self.download_dir,
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    "safebrowsing.enabled": True,
                    "safebrowsing.disable_download_protection": True,
                    "plugins.always_open_pdf_externally": True,
                    "download.open_pdf_in_system_reader": False,
                    "profile.default_content_settings.popups": 0,
                    "profile.default_content_setting_values.automatic_downloads": 1
                }
                options.add_experimental_option("prefs", prefs)

                # Headless mode configuration
                if self.headless:
                    logger.info("Running in headless mode")
                    options.add_argument("--headless=new")  # Use new headless mode
                    options.add_argument("--disable-gpu")
                    options.add_argument("--window-size=1920,1080")
                else:
                    logger.info("Running in visible mode")

                # Additional options for stability
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--disable-web-security")
                options.add_argument("--allow-running-insecure-content")

                # Initialize driver based on strategy
                if strategy_name == 'custom_driver':
                    self.driver = uc.Chrome(options=options, driver_executable_path=param, use_subprocess=False)
                elif strategy_name == 'subprocess':
                    self.driver = uc.Chrome(options=options, use_subprocess=True)
                elif strategy_name == 'detected_version':
                    self.driver = uc.Chrome(options=options, version_main=param, use_subprocess=False)
                else:
                    self.driver = uc.Chrome(options=options, use_subprocess=False)

                if not self.headless:
                    self.driver.maximize_window()

                # Verify and set download directory via JavaScript
                self.set_download_directory()

                logger.info(f"Chrome driver initialized successfully using strategy: {strategy_name}")
                return True

            except Exception as e:
                logger.warning(f"Strategy '{strategy_name}' failed: {str(e)}")
                # Close driver if it was partially created
                try:
                    if self.driver:
                        self.driver.quit()
                        self.driver = None
                except:
                    pass
                continue

        logger.error("All initialization strategies failed")
        return False
    
    def navigate_to_page(self):
        """Navigate to the IMF page and refresh"""
        try:
            logger.info(f"Navigating to {self.url}")
            self.driver.get(self.url)
            
            # Refresh the page as per instructions (Ctrl+F5)
            self.driver.refresh()
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "memberkey1"))
            )
            
            logger.info("Successfully loaded the IMF page")
            return True
            
        except TimeoutException:
            logger.error("Timeout waiting for page to load")
            return False
        except Exception as e:
            logger.error(f"Error navigating to page: {str(e)}")
            return False
    
    def select_country(self, country_name, country_value):
        """
        Select a country from the dropdown
        
        Args:
            country_name (str): Name of the country
            country_value (str): Value attribute for the country option
        """
        try:
            logger.info(f"Selecting country: {country_name}")
            
            # Find the dropdown element
            dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "memberkey1"))
            )
            
            # Create Select object and select by value
            select = Select(dropdown)
            select.select_by_value(country_value)
            
            # Verify selection
            selected_option = select.first_selected_option
            logger.info(f"Selected: {selected_option.text}")
            
            return True
            
        except TimeoutException:
            logger.error("Timeout waiting for dropdown element")
            return False
        except Exception as e:
            logger.error(f"Error selecting country {country_name}: {str(e)}")
            return False
    
    def get_current_selected_date(self):
        """
        Get the currently selected date from the date dropdown without changing it
        
        Returns:
            str: The currently selected date value, or None if failed
        """
        try:
            # Find the date dropdown element
            date_dropdown = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "date1Key"))
            )
            
            # Create Select object and get current selection
            select = Select(date_dropdown)
            selected_option = select.first_selected_option
            selected_date_value = selected_option.get_attribute("value")
            
            logger.info(f"Current selected date: {selected_option.text} (value: {selected_date_value})")
            return selected_date_value
            
        except TimeoutException:
            logger.error("Timeout waiting for date dropdown element")
            return None
        except Exception as e:
            logger.error(f"Error getting current selected date: {str(e)}")
            return None
    
    def submit_form(self):
        """Click the Go button to submit the form"""
        try:
            logger.info("Clicking Go button")
            
            # Find and click the Go button
            go_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit'][value='Go']"))
            )
            
            go_button.click()
            
            # Wait for the new page to load
            time.sleep(3)
            
            logger.info("Form submitted successfully")
            return True
            
        except TimeoutException:
            logger.error("Timeout waiting for Go button")
            return False
        except Exception as e:
            logger.error(f"Error submitting form: {str(e)}")
            return False
    
    def download_tsv(self, country_name, date_value):
        """
        Download the TSV file from the results page and rename it
        
        Args:
            country_name (str): Name of the country for logging and file naming
            date_value (str): The date value to include in filename
        """
        try:
            logger.info(f"Looking for TSV download link for {country_name}")
            
            # Wait for the TSV link to be present
            tsv_link = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.LINK_TEXT, "TSV"))
            )
            
            # Get the href before clicking (for logging)
            tsv_url = tsv_link.get_attribute("href")
            logger.info(f"Found TSV link: {tsv_url}")
            
            # Get list of files before download to identify the new file
            files_before = set(os.listdir(self.download_dir)) if os.path.exists(self.download_dir) else set()
            
            # Try to scroll to element and make it visible
            self.driver.execute_script("arguments[0].scrollIntoView(true);", tsv_link)
            time.sleep(1)
            
            # Try clicking with JavaScript if regular click fails
            try:
                tsv_link.click()
            except Exception as click_error:
                logger.warning(f"Regular click failed, trying JavaScript click: {str(click_error)}")
                self.driver.execute_script("arguments[0].click();", tsv_link)
            
            # Wait for download to complete and find the new file
            max_wait_time = 30  # seconds
            wait_time = 0
            new_file = None
            
            while wait_time < max_wait_time:
                time.sleep(2)
                wait_time += 2
                
                if os.path.exists(self.download_dir):
                    files_after = set(os.listdir(self.download_dir))
                    new_files = files_after - files_before
                    
                    if new_files:
                        # Find the TSV file (should end with .tsv)
                        tsv_files = [f for f in new_files if f.lower().endswith('.tsv')]
                        if tsv_files:
                            new_file = tsv_files[0]
                            break
                        # If no .tsv extension, take any new file
                        elif new_files:
                            new_file = list(new_files)[0]
                            break
            
            if new_file:
                # Rename the file to include country name and date, preserving original extension
                old_path = os.path.join(self.download_dir, new_file)
                
                # Extract original file extension
                if '.' in new_file:
                    file_ext = '.' + new_file.split('.')[-1]
                else:
                    file_ext = ''  # Keep original format if no extension
                
                new_filename = f"{country_name}_IMF_External_Arrangements_{date_value}{file_ext}"
                new_path = os.path.join(self.download_dir, new_filename)
                
                try:
                    os.rename(old_path, new_path)
                    logger.info(f"File renamed to: {new_filename}")
                except Exception as rename_error:
                    logger.warning(f"Could not rename file: {str(rename_error)}")
                    logger.info(f"File saved as: {new_file}")
            else:
                logger.warning(f"Could not identify downloaded file for {country_name}")
            
            logger.info(f"TSV download completed for {country_name}")
            return True
            
        except TimeoutException:
            logger.error(f"Timeout waiting for TSV link for {country_name}")
            return False
        except Exception as e:
            logger.error(f"Error downloading TSV for {country_name}: {str(e)}")
            return False
    
    def download_tsv_direct(self, country_name, date_value):
        """
        Alternative download method: get TSV URL and download directly with requests
        
        Args:
            country_name (str): Name of the country for file naming
            date_value (str): The date value to include in filename
        """
        try:
            logger.info(f"Looking for TSV download link for {country_name}")
            
            # Wait for the TSV link to be present
            tsv_link = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.LINK_TEXT, "TSV"))
            )
            
            # Get the href URL
            tsv_url = tsv_link.get_attribute("href")
            logger.info(f"Found TSV link: {tsv_url}")
            
            # Make the URL absolute if it's relative
            if tsv_url.startswith("extarr2.aspx"):
                tsv_url = f"https://www.imf.org/external/np/fin/tad/{tsv_url}"
            
            # Download the file using requests
            logger.info(f"Downloading file directly from: {tsv_url}")
            
            # Get cookies from selenium session for requests
            cookies = {}
            for cookie in self.driver.get_cookies():
                cookies[cookie['name']] = cookie['value']
            
            response = requests.get(tsv_url, cookies=cookies, timeout=30)
            response.raise_for_status()
            
            # Get the original filename from the URL or use default
            original_filename = tsv_url.split('/')[-1] if '/' in tsv_url else "extarr2.aspx"
            
            # Extract file extension from original or default to the original format
            if '.' in original_filename:
                file_ext = '.' + original_filename.split('.')[-1]
            else:
                file_ext = ''  # Keep original format without extension
            
            # Create filename with country prefix but keep original extension
            filename = f"{country_name}_IMF_External_Arrangements_{date_value}{file_ext}"
            filepath = os.path.join(self.download_dir, filename)
            
            # Save the file
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"File saved successfully as: {filename}")
            logger.info(f"File size: {len(response.content)} bytes")
            
            return True
            
        except TimeoutException:
            logger.error(f"Timeout waiting for TSV link for {country_name}")
            return False
        except requests.RequestException as e:
            logger.error(f"Request error downloading TSV for {country_name}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error downloading TSV for {country_name}: {str(e)}")
            return False
    
    def process_country(self, country_name, country_value):
        """
        Process a single country: select country, get current date, submit, and download
        
        Args:
            country_name (str): Name of the country
            country_value (str): Value attribute for the country option
        """
        logger.info(f"Processing {country_name}...")
        
        # Navigate to the main page
        if not self.navigate_to_page():
            return False
        
        # Select the country
        if not self.select_country(country_name, country_value):
            return False
        
        # Get the current selected date (don't change it)
        current_date = self.get_current_selected_date()
        if not current_date:
            logger.warning("Could not get current date, using default")
            current_date = "unknown_date"
        
        # Submit the form
        if not self.submit_form():
            return False
        
        # Download the TSV file using direct method (more reliable)
        if not self.download_tsv_direct(country_name, current_date):
            logger.warning("Direct download failed, trying browser download method...")
            if not self.download_tsv(country_name, current_date):
                return False
        
        logger.info(f"Successfully processed {country_name}")
        return True
    
    def run(self):
        """Main method to run the scraper for all countries"""
        logger.info("Starting IMF IMFEOD data scraper")
        logger.info(f"Download directory: {self.download_dir}")
        logger.info(f"Headless mode: {self.headless}")
        
        # Set up the driver
        if not self.setup_driver():
            logger.error("Failed to set up driver. Exiting.")
            return False
        
        try:
            successful_downloads = 0
            total_countries = len(self.countries)
            
            # Process each country with retry logic
            for country_name, country_value in self.countries.items():
                logger.info(f"Processing {country_name} ({country_value})")
                
                # Try up to 2 times per country
                success = False
                for attempt in range(1, 3):
                    if attempt > 1:
                        logger.info(f"Retrying {country_name} (attempt {attempt}/2) - reloading page first...")
                        # Reload the main page before retry
                        self.driver.get(self.url)
                        time.sleep(2)
                    
                    if self.process_country(country_name, country_value):
                        successful_downloads += 1
                        logger.info(f"✓ {country_name} completed successfully")
                        success = True
                        break
                    else:
                        logger.warning(f"✗ {country_name} failed on attempt {attempt}")
                
                if not success:
                    logger.error(f"✗ {country_name} failed after all retry attempts")
                
                # Wait between countries to avoid overwhelming the server
                if country_name != list(self.countries.keys())[-1]:  # Not the last country
                    logger.info("Waiting 3 seconds before next country...")
                    time.sleep(3)
            
            # Summary
            logger.info(f"Scraping completed: {successful_downloads}/{total_countries} countries processed successfully")
            logger.info(f"Downloaded files should be in: {self.download_dir}")
            
            return successful_downloads == total_countries
            
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {str(e)}")
            return False
        
        finally:
            # Clean up
            if self.driver:
                logger.info("Closing browser")
                self.driver.quit()
    
    def list_downloaded_files(self):
        """List all files in the download directory"""
        try:
            files = os.listdir(self.download_dir)
            if files:
                logger.info("Downloaded files:")
                for file in files:
                    file_path = os.path.join(self.download_dir, file)
                    file_size = os.path.getsize(file_path)
                    logger.info(f"  - {file} ({file_size} bytes)")
            else:
                logger.info("No files found in download directory")
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")


def main():
    """Main function to run the scraper"""
    # Use simple downloads folder
    download_dir = "downloads"
    
    # Initialize and run scraper with headless mode setting
    scraper = IMFScraper(download_dir, headless=HEADLESS_MODE)
    
    try:
        success = scraper.run()
        
        # List downloaded files
        scraper.list_downloaded_files()
        
        if success:
            print("\n✓ All countries processed successfully!")
        else:
            print("\n⚠ Some countries failed. Check the logs above.")
            
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
        if scraper.driver:
            scraper.driver.quit()
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")


if __name__ == "__main__":
    main()