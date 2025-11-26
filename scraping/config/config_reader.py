from scraping.config import model
from scraping import coordinator
import bittensor as bt

class ConfigReader:
    """A class to read the scraping config from a json file."""
    
    @classmethod
    def load_config(cls, filepath: str) -> coordinator.CoordinatorConfig:
        """Loads the scraping config from json and returns it as a CoordinatorConfig.
        
        Raises:
            ValidationError: if the file content is not valid.
        """
        
        bt.logging.trace(f"Loading file: {filepath}")
        parsed_file = model.ScrapingConfig.parse_file(path=filepath)
        bt.logging.trace(f"Got parsed file: {parsed_file}")
        return parsed_file.to_coordinator_config()
    
    @classmethod
    def load_multi_account_config(cls, filepath: str) -> model.MultiAccountScrapingConfig:
        """Loads a multi-account ScrapingConfig."""
        try:
            bt.logging.trace(f"🔍 Loading multi-account config from: {filepath}")
            return model.MultiAccountScrapingConfig.parse_file(path=filepath)
        except Exception as e:
            bt.logging.error(f"❌ Failed to parse multi-account config: {e}")
            raise
    
    @classmethod
    def load_account_config(cls, scraper_config: model.ScraperConfig) -> coordinator.CoordinatorConfig:
  
        # Build a CoordinatorConfig manually
        return coordinator.CoordinatorConfig(
            scraper_configs={
                scraper_config.scraper_id: scraper_config.to_coordinator_scraper_config()
            }
        )