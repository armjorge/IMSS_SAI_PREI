import os
import zipfile
import platform
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


class WebAutomationDriver:
    def __init__(self, downloads_path):
        self.downloads_path = Path(downloads_path)
        self.system = platform.system()
        self.home = Path.home()
        self._validate_downloads_path()
        self._set_chrome_paths()
    
    def _validate_downloads_path(self):
        """Validate that the downloads path exists and is a directory."""
        if not self.downloads_path.exists():
            self.downloads_path.mkdir(parents=True, exist_ok=True)
        if not self.downloads_path.is_dir():
            raise NotADirectoryError(f"Downloads path is not a directory: {self.downloads_path}")
    
    def _set_chrome_paths(self):
        """Set Chrome binary and ChromeDriver paths based on OS."""
        if self.system == "Windows":
            self.chrome_binary_path = self.home / "Documents" / "chrome-win64" / "chrome.exe"
            self.chromedriver_path = self.home / "Documents" / "chromedriver-win64" / "chromedriver.exe"
        elif self.system == "Darwin":  # macOS
            self.chrome_binary_path = (
                self.home / "chrome_testing" / "chrome-mac-arm64" / 
                "Google Chrome for Testing.app" / "Contents" / "MacOS" / 
                "Google Chrome for Testing"
            )
            self.chromedriver_path = (
                self.home / "chrome_testing" / "chromedriver-mac-arm64" / "chromedriver"
            )
        else:
            raise OSError(f"Unsupported OS: {self.system}")
    
    def _check_chrome_installation(self):
        """Check if Chrome and ChromeDriver are installed."""
        chrome_exists = self.chrome_binary_path.exists()
        chromedriver_exists = self.chromedriver_path.exists()
        
        if not chrome_exists:
            print(f"❌ Chrome binary not found at: {self.chrome_binary_path}")
        else:
            print(f"✅ Chrome binary found at: {self.chrome_binary_path}")
            
        if not chromedriver_exists:
            print(f"❌ ChromeDriver not found at: {self.chromedriver_path}")
        else:
            print(f"✅ ChromeDriver found at: {self.chromedriver_path}")
            
        return chrome_exists and chromedriver_exists
    
    def _install_chrome_macos(self):
        """Install Chrome and ChromeDriver for macOS."""
        print("Installing Chrome for Testing and ChromeDriver for macOS...")
        print("Open https://googlechromelabs.github.io/chrome-for-testing/ to download the files.")
        
        download_dir = self.home / "Downloads"
        target_dir = self.home / "chrome_testing"
        
        chromedriver_zip = download_dir / "chromedriver-mac-arm64.zip"
        chrome_zip = download_dir / "chrome-mac-arm64.zip"
        
        # Loop until both zip files are found
        while True:
            found_chromedriver = chromedriver_zip.exists()
            found_chrome = chrome_zip.exists()
            
            if found_chromedriver:
                print("✅ chromedriver zip found")
            else:
                print("⏳ chromedriver zip not found. Please download it and press Enter to retry.")
                input()
                
            if found_chrome:
                print("✅ chrome zip found")
            else:
                print("⏳ chrome zip not found. Please download it and press Enter to retry.")
                input()
                
            if found_chromedriver and found_chrome:
                print("✅ Both files found.")
                break
        
        # Create target directory
        target_dir.mkdir(exist_ok=True)
        
        # Extract files
        def unzip_to_target(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
                print(f"✅ Extracted {zip_path.name}")
        
        unzip_to_target(chromedriver_zip)
        unzip_to_target(chrome_zip)
        
        # Make chromedriver executable
        if self.chromedriver_path.exists():
            os.chmod(self.chromedriver_path, 0o755)
            print(f"✅ Made chromedriver executable")
        
        # Final verification
        if self._check_chrome_installation():
            print("✅ Chrome installation completed successfully!")
        else:
            print("❌ Chrome installation failed!")
    
    def _install_chrome_windows(self):
        """Install Chrome and ChromeDriver for Windows."""
        print("For Windows, please manually download Chrome for Testing and ChromeDriver from:")
        print("https://googlechromelabs.github.io/chrome-for-testing/")
        print(f"Extract chrome-win64.zip to: {self.home / 'Documents'}")
        print(f"Extract chromedriver-win64.zip to: {self.home / 'Documents'}")
        input("Press Enter when installation is complete...")
    
    def ensure_chrome_installed(self):
        """Ensure Chrome and ChromeDriver are installed."""
        if self._check_chrome_installation():
            return True
        
        print("Chrome or ChromeDriver not found. Installing...")
        
        if self.system == "Darwin":
            self._install_chrome_macos()
        elif self.system == "Windows":
            self._install_chrome_windows()
        
        return self._check_chrome_installation()
    
    def create_driver(self, custom_downloads_path=None):
        """Create and return a Chrome WebDriver instance."""
        if not self.ensure_chrome_installed():
            raise RuntimeError("Failed to install or locate Chrome components")
        
        # Use custom path if provided, otherwise use instance path
        downloads_path = Path(custom_downloads_path) if custom_downloads_path else self.downloads_path
        
        # Ensure the downloads directory exists
        downloads_path.mkdir(parents=True, exist_ok=True)
        
        # Set Chrome options
        chrome_options = Options()
        chrome_options.binary_location = str(self.chrome_binary_path)
        
        prefs = {
            "download.default_directory": str(downloads_path.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        # Performance and stability options
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        
        try:
            service = Service(str(self.chromedriver_path))
            driver = webdriver.Chrome(service=service, options=chrome_options)
            print(f"✅ Chrome driver created successfully with downloads path: {self.downloads_path}")
            return driver
        except Exception as e:
            print(f"❌ Failed to initialize Chrome driver: {e}")
            raise
    
    def get_downloads_path(self):
        """Return the downloads path."""
        return self.downloads_path
    
    def list_downloaded_files(self):
        """List all files in the downloads directory."""
        return list(self.downloads_path.iterdir())