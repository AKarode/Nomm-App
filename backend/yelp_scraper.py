import requests
import time
import logging
from supabase import create_client, Client
from typing import List, Dict, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Restaurant:
    name: str
    rating: Optional[float]
    review_count: Optional[int]
    price_range: Optional[str]
    yelp_id: str
    website: Optional[str]
    address: Optional[str]
    phone: Optional[str]
    cuisine_type: Optional[str]

class YelpMenuScraper:
    def __init__(self, yelp_api_key: str, supabase_url: str, supabase_key: str):
        self.yelp_api_key = yelp_api_key
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.headers = {
            'Authorization': f'Bearer {yelp_api_key}',
            'Accept': 'application/json'
        }
        # Headers for web scraping
        self.scraping_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
    def search_restaurants_batch(self, location: str = "San Ramon, CA", total_limit: int = 200) -> List[Dict]:
        """Search for restaurants using Yelp API with batching to get more than 50 results"""
        url = "https://api.yelp.com/v3/businesses/search"
        all_businesses = []
        
        # Calculate number of batches needed
        batch_size = 50  # Yelp API max per request
        num_batches = (total_limit + batch_size - 1) // batch_size  # Ceiling division
        
        logger.info(f"Starting batch search for {total_limit} restaurants in {num_batches} batches")
        
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            limit = min(batch_size, total_limit - len(all_businesses))
            
            if limit <= 0:
                break
                
            params = {
                'term': 'restaurants',
                'location': location,
                'limit': limit,
                'offset': offset,
                'categories': 'restaurants'
            }
            
            try:
                logger.info(f"Fetching batch {batch_num + 1}/{num_batches} (offset: {offset}, limit: {limit})")
                
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                businesses = data.get('businesses', [])
                
                if not businesses:
                    logger.warning(f"No businesses returned in batch {batch_num + 1}")
                    break
                
                all_businesses.extend(businesses)
                logger.info(f"Batch {batch_num + 1} returned {len(businesses)} restaurants. Total so far: {len(all_businesses)}")
                
                # Rate limiting between batches
                if batch_num < num_batches - 1:  # Don't sleep after the last batch
                    logger.info("Waiting 1 second between batches...")
                    time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in batch {batch_num + 1}: {e}")
                # Continue with next batch instead of failing completely
                continue
        
        logger.info(f"Completed batch search. Total restaurants found: {len(all_businesses)}")
        return all_businesses
    
    def get_restaurant_details(self, business_id: str) -> Optional[Dict]:
        """Get detailed restaurant information using Yelp API"""
        url = f"https://api.yelp.com/v3/businesses/{business_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting restaurant details for {business_id}: {e}")
            return None
    
    def convert_to_restaurant(self, business_data: Dict) -> Restaurant:
        """Convert Yelp API data to Restaurant object"""
        # Handle address
        address = None
        if business_data.get('location', {}).get('display_address'):
            address = ', '.join(business_data['location']['display_address'])
        
        # Handle categories/cuisine type
        cuisine_type = None
        if business_data.get('categories'):
            cuisine_type = ', '.join([cat['title'] for cat in business_data['categories']])
        
        # Handle price range
        price_range = business_data.get('price', None)
        
        return Restaurant(
            name=business_data.get('name', 'Unknown'),
            rating=business_data.get('rating'),
            review_count=business_data.get('review_count'),
            price_range=price_range,
            yelp_id=business_data.get('id', ''),
            website=business_data.get('url'),  # This is the Yelp URL
            address=address,
            phone=business_data.get('phone'),
            cuisine_type=cuisine_type
        )
    
    def construct_menu_url(self, restaurant: Restaurant) -> str:
        """Construct the Yelp menu URL from restaurant data"""
        # Convert restaurant name to URL-friendly format
        business_name = restaurant.name.lower()
        # Replace spaces and special characters with hyphens
        business_name = re.sub(r'[^a-z0-9]+', '-', business_name)
        # Remove leading/trailing hyphens and multiple consecutive hyphens
        business_name = re.sub(r'-+', '-', business_name).strip('-')
        
        # Get location from address for URL construction
        location = "san-ramon"  # Default location
        if restaurant.address:
            # Try to extract city from address
            address_parts = restaurant.address.lower().split(',')
            for part in address_parts:
                part = part.strip()
                if 'san ramon' in part:
                    location = "san-ramon"
                elif 'dublin' in part:
                    location = "dublin"
                elif 'pleasanton' in part:
                    location = "pleasanton"
                elif 'livermore' in part:
                    location = "livermore"
                elif 'castro valley' in part:
                    location = "castro-valley"
                elif 'hayward' in part:
                    location = "hayward"
        
        # Construct menu URL using the proper format
        menu_url = f"https://www.yelp.com/menu/{business_name}-{location}"
        return menu_url
    
    def scrape_menu_from_yelp(self, menu_url: str) -> List[Dict]:
        """Scrape menu items from Yelp menu page"""
        try:
            logger.info(f"Attempting to scrape menu from: {menu_url}")
            
            response = requests.get(menu_url, headers=self.scraping_headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            menu_items = []
            
            # Try multiple selectors for Yelp menu items
            selectors_to_try = [
                'div.menu-item',
                '[data-testid="menu-item"]',
                '.menu-item-details',
                '.menuItem',
                '.biz-menu-item'
            ]
            
            for selector in selectors_to_try:
                items = soup.select(selector)
                if items:
                    logger.info(f"Found {len(items)} menu items using selector: {selector}")
                    break
            
            if not items:
                logger.warning("No menu items found with any selector")
                return []
            
            for item in items:
                try:
                    # Try different ways to extract item name
                    item_name = ''
                    name_selectors = ['h4', '.menu-item-name', '.item-name', 'h3', 'strong']
                    for name_sel in name_selectors:
                        name_elem = item.select_one(name_sel)
                        if name_elem:
                            item_name = name_elem.get_text(strip=True)
                            break
                    
                    # Try different ways to extract description
                    description = ''
                    desc_selectors = [
                        '.menu-item-details-description',
                        '.menu-item-description', 
                        '.item-description',
                        'p'
                    ]
                    for desc_sel in desc_selectors:
                        desc_elem = item.select_one(desc_sel)
                        if desc_elem:
                            description = desc_elem.get_text(strip=True)
                            break
                    
                    # Try different ways to extract price
                    price = ''
                    price_selectors = [
                        '.menu-item-price-amount',
                        '.menu-item-price',
                        '.item-price',
                        '.price'
                    ]
                    for price_sel in price_selectors:
                        price_elem = item.select_one(price_sel)
                        if price_elem:
                            price_text = price_elem.get_text(strip=True)
                            # Extract price using regex
                            price_match = re.search(r'\$[\d,]+\.?\d*', price_text)
                            if price_match:
                                price = price_match.group()
                            break
                    
                    # Only add if we have at least name and some other info
                    if item_name and (description or price):
                        menu_items.append({
                            'name': item_name,
                            'description': description,
                            'price': price
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing menu item: {e}")
                    continue
            
            logger.info(f"Successfully scraped {len(menu_items)} menu items")
            return menu_items
            
        except requests.RequestException as e:
            logger.error(f"Error fetching menu page {menu_url}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error scraping menu from {menu_url}: {e}")
            return []
    
    def get_menu_data(self, restaurant: Restaurant) -> Optional[tuple[List[Dict], List[Dict]]]:
        """Get real menu data or return None if no menu found"""
        # Try to scrape real menu
        menu_url = self.construct_menu_url(restaurant)
        scraped_dishes = self.scrape_menu_from_yelp(menu_url)
        
        if scraped_dishes:
            logger.info(f"Found menu data for {restaurant.name}")
            menu = {
                'name': f"{restaurant.name} Menu",
                'description': f"Main menu for {restaurant.name}",
                'menu_type': 'main',
                'display_order': 0
            }
            return [menu], scraped_dishes
        else:
            logger.info(f"No menu data found for {restaurant.name} - skipping")
            return None
    
    def clean_price(self, price_str: str) -> Optional[float]:
        """Clean and convert price string to float"""
        if not price_str:
            return None
        
        # Remove everything except digits and decimal point
        cleaned = re.sub(r'[^\d.]', '', price_str)
        
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None
    
    def save_to_database(self, restaurant: Restaurant, menus: List[Dict], dishes: List[Dict]):
        """Save restaurant, menu, and dish data to Supabase"""
        try:
            # Insert restaurant
            restaurant_data = {
                'name': restaurant.name,
                'rating': restaurant.rating,
                'review_count': restaurant.review_count,
                'price_range': restaurant.price_range,
                'yelp_id': restaurant.yelp_id,
                'website': restaurant.website,
                'address': restaurant.address,
                'phone': restaurant.phone,
                'cuisine_type': restaurant.cuisine_type
            }
            
            # Check if restaurant already exists
            existing = self.supabase.table('restaurant').select('id').eq('yelp_id', restaurant.yelp_id).execute()
            
            if existing.data:
                restaurant_id = existing.data[0]['id']
                logger.info(f"Restaurant {restaurant.name} already exists with ID {restaurant_id}")
            else:
                result = self.supabase.table('restaurant').insert(restaurant_data).execute()
                restaurant_id = result.data[0]['id']
                logger.info(f"Inserted restaurant {restaurant.name} with ID {restaurant_id}")
            
            # Insert menus
            for menu_data in menus:
                menu_data['restaurant_id'] = restaurant_id
                
                menu_result = self.supabase.table('menu').insert(menu_data).execute()
                menu_id = menu_result.data[0]['id']
                logger.info(f"Inserted menu {menu_data['name']} with ID {menu_id}")
                
                # Insert dishes for this menu
                for dish_data in dishes:
                    # Clean the price
                    cleaned_price = self.clean_price(dish_data.get('price', ''))
                    
                    dish_insert_data = {
                        'menu_id': menu_id,
                        'name': dish_data['name'],
                        'description': dish_data.get('description', ''),
                        'price': cleaned_price,
                        'display_order': 0
                    }
                    
                    self.supabase.table('dish').insert(dish_insert_data).execute()
                
                logger.info(f"Inserted {len(dishes)} dishes for menu {menu_data['name']}")
                
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
    
    def collect_san_ramon_data(self, total_restaurants: int = 200):
        """Main method to collect San Ramon restaurant data with real menu scraping only"""
        logger.info(f"Starting San Ramon restaurant data collection for {total_restaurants} restaurants - skipping restaurants without menu data...")
        
        # Estimate time for user
        estimated_minutes = (total_restaurants * 4) // 60  # Rough estimate: 4 seconds per restaurant
        logger.info(f"Estimated completion time: ~{estimated_minutes} minutes")
        
        # Search for restaurants using batch processing
        businesses = self.search_restaurants_batch("San Ramon, CA", total_restaurants)
        
        if not businesses:
            logger.error("No businesses found. Exiting.")
            return
        
        logger.info(f"Processing {len(businesses)} restaurants...")
        
        restaurants_processed = 0
        restaurants_skipped = 0
        
        for idx, business in enumerate(businesses):
            logger.info(f"Processing restaurant {idx + 1}/{len(businesses)}: {business['name']}")
            
            # Get detailed information
            detailed_info = self.get_restaurant_details(business['id'])
            if detailed_info:
                business.update(detailed_info)
            
            # Convert to Restaurant object
            restaurant = self.convert_to_restaurant(business)
            
            # Try to get menu data
            menu_data = self.get_menu_data(restaurant)
            
            if menu_data is not None:
                # We found menu data, save to database
                menus, dishes = menu_data
                self.save_to_database(restaurant, menus, dishes)
                restaurants_processed += 1
                logger.info(f"✓ Successfully processed {restaurant.name} with menu data")
            else:
                # No menu data found, skip this restaurant
                restaurants_skipped += 1
                logger.info(f"✗ Skipped {restaurant.name} - no menu data available")
            
            # Rate limiting - be respectful to Yelp
            time.sleep(2)
            
            # Log progress every 25 restaurants for 1000+ restaurants, every 10 for smaller batches
            progress_interval = 25 if total_restaurants >= 500 else 10
            if (idx + 1) % progress_interval == 0:
                percent_complete = ((idx + 1) / len(businesses)) * 100
                logger.info(f"Progress: {idx + 1}/{len(businesses)} restaurants checked ({percent_complete:.1f}%) | {restaurants_processed} processed | {restaurants_skipped} skipped")
            
        logger.info(f"Data collection completed!")
        logger.info(f"Total restaurants checked: {len(businesses)}")
        logger.info(f"Restaurants processed with menu data: {restaurants_processed}")
        logger.info(f"Restaurants skipped (no menu data): {restaurants_skipped}")

def main():
    # Configuration - Add your API keys here
    YELP_API_KEY = "wm9babvXv1Vmng-6r3cZ3wBhx6CalFjlVMvkARZxG7EGzlI1j3W1ccPSRK04vFHTENBo2RePWvxtAvP7Hm5pvXVr6VAYBB0yGOi4aIkYv4hdySQdc2oM13KDJO5aaHYx"
    SUPABASE_URL = "https://jbryyaantwdtwowxcltx.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impicnl5YWFudHdkdHdvd3hjbHR4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA3MTk2MDksImV4cCI6MjA2NjI5NTYwOX0.TJdo0-7dTgEKdbekuKp42TsTiEKcvBsFaI7BsUzCwGA"
    
    if not YELP_API_KEY or YELP_API_KEY == "your-yelp-api-key-here":
        print("Please get a Yelp API key from https://www.yelp.com/developers")
        return
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Please update SUPABASE_URL and SUPABASE_KEY in the code")
        return
    
    # Initialize scraper
    scraper = YelpMenuScraper(YELP_API_KEY, SUPABASE_URL, SUPABASE_KEY)
    
    # Start data collection for 1000 restaurants
    scraper.collect_san_ramon_data(total_restaurants=1000)

if __name__ == "__main__":
    main()