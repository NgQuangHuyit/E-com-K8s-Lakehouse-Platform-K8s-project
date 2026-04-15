# gen_clickstream.py
import json
import uuid
import random
import datetime
import argparse
import csv
from pathlib import Path
from collections import deque
import mysql.connector
from mysql.connector import Error
import os

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("⚠️  tqdm not installed. Install with: pip install tqdm")

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'oltp'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

ORDER_BATCH_SIZE = 500  # Bulk insert orders every 500 purchases

# Enhanced Configuration with Weighted Distributions

# ==================== DATABASE CONNECTION ====================

def create_db_connection():
    """Create MySQL database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"⚠️  Database connection error: {e}")
        print("⚠️  Falling back to CSV data loading...")
        return None

def load_categories_from_db(connection):
    """Load categories from MySQL database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT category_id, category_display_name, category_description FROM categories")
        categories = {}
        for row in cursor.fetchall():
            categories[row['category_id']] = {
                'id': row['category_id'],
                'name': row['category_display_name'],
                'description': row['category_description']
            }
        cursor.close()
        print(f"✅ Loaded {len(categories)} categories from database")
        return categories
    except Error as e:
        print(f"⚠️  Error loading categories: {e}")
        return None

def load_products_from_db(connection):
    """Load products from MySQL database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT product_id, product_name, product_description, price, 
                   category_id, brand_id, created_at, updated_at 
            FROM products
        """)
        products = {}
        for row in cursor.fetchall():
            products[row['product_id']] = {
                'product_id': row['product_id'],
                'name': row['product_name'],
                'description': row['product_description'],
                'price': float(row['price']),
                'category_id': row['category_id'],
                'brand_id': row['brand_id'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            }
        cursor.close()
        print(f"✅ Loaded {len(products)} products from database")
        return products
    except Error as e:
        print(f"⚠️  Error loading products: {e}")
        return None

def load_customers_from_db(connection):
    """Load customer IDs from MySQL database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"✅ Loaded {len(customer_ids)} customers from database")
        return customer_ids
    except Error as e:
        print(f"⚠️  Error loading customers: {e}")
        return None

def load_payment_methods_from_db(connection):
    """Load payment methods from MySQL database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT payment_method_id, display_name FROM payment_methods")
        payment_methods = cursor.fetchall()
        cursor.close()
        print(f"✅ Loaded {len(payment_methods)} payment methods from database")
        return payment_methods
    except Error as e:
        print(f"⚠️  Error loading payment methods: {e}")
        return None

# ==================== CSV DATA LOADERS (FALLBACK) ====================

def load_categories(csv_path="metadata/category (1).csv"):
    """Load categories from CSV file"""
    categories = {}
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                categories[int(row['category_id'])] = {
                    'id': int(row['category_id']),
                    'name': row['category_display_name'],
                    'description': row['category_description']
                }
    except FileNotFoundError:
        print(f"⚠️  Warning: {csv_path} not found. Using default categories.")
        return None
    return categories

def load_products(csv_path="metadata/products (1).csv"):
    """Load products from CSV file"""
    products = {}
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                products[int(row['product_id'])] = {
                    'product_id': int(row['product_id']),
                    'name': row['product_name'],
                    'description': row['product_description'],
                    'price': float(row['unit_price']),
                    'category_id': int(row['category_id']),
                    'brand_id': row['brand_id'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
    except FileNotFoundError:
        print(f"⚠️  Warning: {csv_path} not found. Using synthetic products.")
        return None
    return products

# Category popularity weights based on Vietnamese e-commerce (higher = more traffic)
CATEGORY_WEIGHTS = {
    7361: 25,   # Điện thoại & Máy tính bảng - High demand
    6809: 22,   # Laptop & Máy tính - High demand
    4130: 15,   # Thời trang nam
    1964: 15,   # Thời trang nữ
    9290: 12,   # Mỹ phẩm & Làm đẹp
    7907: 8,    # Điện tử gia dụng
    6271: 6,    # Đồ gia dụng
    6978: 4,    # Xe cộ & Phụ kiện
    6229: 3,    # Thể thao & Dã ngoại
    6257: 2,    # Sách & Văn phòng phẩm
    8481: 2,    # Đồ chơi & Mẹ bé
    8704: 2,    # Y tế & Sức khỏe
    3130: 1,    # Thiết bị giáo dục
    6815: 2,    # Thực phẩm & Đồ uống
    8386: 1,    # Trang sức & Đồng hồ
    7431: 1,    # Vật nuôi & Thú cưng
    1167: 1,    # Nội thất & Trang trí
    2978: 1,    # Công cụ & Dụng cụ
    3669: 2,    # Game & Giải trí
    9686: 1     # Dịch vụ số & Gói đăng ký
}

ACTION_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist", "review", "compare"]

# Device distribution with weights (mobile-first market)
DEVICES = [
    {"os": "Android", "browser": "Chrome", "device_type": "mobile", "version": "12.0", "weight": 30},
    {"os": "Android", "browser": "Firefox", "device_type": "mobile", "version": "11.0", "weight": 5},
    {"os": "iOS", "browser": "Safari", "device_type": "mobile", "version": "16.0", "weight": 25},
    {"os": "iOS", "browser": "Chrome", "device_type": "mobile", "version": "15.0", "weight": 10},
    {"os": "Windows", "browser": "Chrome", "device_type": "desktop", "version": "110.0", "weight": 15},
    {"os": "Windows", "browser": "Edge", "device_type": "desktop", "version": "109.0", "weight": 8},
    {"os": "Windows", "browser": "Firefox", "device_type": "desktop", "version": "108.0", "weight": 3},
    {"os": "MacOS", "browser": "Safari", "device_type": "desktop", "version": "16.0", "weight": 2},
    {"os": "MacOS", "browser": "Chrome", "device_type": "desktop", "version": "110.0", "weight": 1.5},
    {"os": "Linux", "browser": "Firefox", "device_type": "desktop", "version": "108.0", "weight": 0.5}
]

# City distribution with weights (population-based)
CITIES = [
    {"name": "Hanoi", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 21.0285, "lon": 105.8542, "weight": 25},
    {"name": "Ho Chi Minh City", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 10.8231, "lon": 106.6297, "weight": 40},
    {"name": "Da Nang", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 16.0544, "lon": 108.2022, "weight": 10},
    {"name": "Hai Phong", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 20.8449, "lon": 106.6881, "weight": 8},
    {"name": "Can Tho", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 10.0452, "lon": 105.7469, "weight": 6},
    {"name": "Bien Hoa", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 10.9470, "lon": 106.8223, "weight": 5},
    {"name": "Hue", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 16.4637, "lon": 107.5909, "weight": 4},
    {"name": "Nha Trang", "country": "VN", "timezone": "Asia/Ho_Chi_Minh", "lat": 12.2388, "lon": 109.1967, "weight": 2}
]

# Referrer distribution with weights (realistic traffic sources)
REFERRERS = [
    {"url": "https://google.com", "type": "search_engine", "weight": 30},
    {"url": "https://facebook.com", "type": "social_media", "weight": 20},
    {"url": "https://instagram.com", "type": "social_media", "weight": 12},
    {"url": "https://youtube.com", "type": "social_media", "weight": 8},
    {"url": "https://tiktok.com", "type": "social_media", "weight": 10},
    {"url": "direct", "type": "direct", "weight": 15},
    {"url": "email", "type": "email", "weight": 4},
    {"url": "https://affiliate-partner.com", "type": "affiliate", "weight": 1}
]

# Campaign distribution with weights (seasonal and ongoing)
CAMPAIGNS = [
    {"name": "summer_sale_2025", "weight": 15},
    {"name": "winter_sale_2025", "weight": 10},
    {"name": "black_friday", "weight": 20},
    {"name": "cyber_monday", "weight": 18},
    {"name": "new_year_promo", "weight": 12},
    {"name": "flash_sale", "weight": 8},
    {"name": "clearance", "weight": 5},
    {"name": "loyalty_rewards", "weight": 6},
    {"name": "first_purchase_discount", "weight": 4},
    {"name": "referral_bonus", "weight": 2},
    {"name": None, "weight": 50}  # No campaign (organic)
]

# Source distribution with weights
SOURCES = [
    {"name": "email", "weight": 15},
    {"name": "paid_ads", "weight": 25},
    {"name": "organic", "weight": 30},
    {"name": "social", "weight": 20},
    {"name": "referral", "weight": 8},
    {"name": "direct", "weight": 2}
]

USER_SEGMENTS = ["new", "returning", "loyal", "vip"]


# ==================== WEIGHTED RANDOM SELECTION ====================

def weighted_random_choice(items, weight_key='weight'):
    """Select item based on weights"""
    if not items:
        return None
    
    # Check if items have weights
    if isinstance(items[0], dict) and weight_key in items[0]:
        weights = [item[weight_key] for item in items]
        return random.choices(items, weights=weights, k=1)[0]
    else:
        return random.choice(items)


# ==================== PRODUCT CATALOG ====================

class ProductCatalog:
    """Manages product metadata from MySQL database with weighted category distribution"""
    def __init__(self, db_connection=None, categories_csv="metadata/category (1).csv", products_csv="metadata/products (1).csv"):
        # Try database first, fallback to CSV
        if db_connection:
            self.categories_data = load_categories_from_db(db_connection)
            self.products_data = load_products_from_db(db_connection)
        else:
            self.categories_data = None
            self.products_data = None
        
        # Fallback to CSV if database loading failed
        if not self.categories_data:
            self.categories_data = load_categories(categories_csv)
        if not self.products_data:
            self.products_data = load_products(products_csv)
        
        if self.products_data:
            self.products = self._enrich_products()
        else:
            # Fallback to synthetic data if CSV not found
            self.products = self._generate_synthetic_catalog()
        
        self.by_category = self._index_by_category()
    
    def _enrich_products(self):
        """Enrich CSV products with computed fields"""
        catalog = {}
        for pid, prod in self.products_data.items():
            # Discount distribution: most products have no discount
            discount = random.choices(
                [0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
                weights=[60, 15, 10, 7, 5, 2, 1],
                k=1
            )[0]
            
            base_price = prod['price']
            category_name = self.categories_data[prod['category_id']]['name'] if self.categories_data else str(prod['category_id'])
            
            catalog[pid] = {
                "product_id": pid,
                "name": prod['name'],
                "description": prod['description'],
                "category": category_name,
                "category_id": prod['category_id'],
                "brand_id": prod['brand_id'],
                "price": round(base_price, 2),
                "discount": discount,
                "final_price": round(base_price * (1 - discount), 2),
                "rating": round(random.uniform(3.5, 5.0), 1),
                "in_stock": random.random() < 0.95
            }
        return catalog
    
    def _generate_synthetic_catalog(self):
        """Fallback: Generate synthetic product metadata if CSV not available"""
        print("⚠️  Generating synthetic product catalog...")
        catalog = {}
        categories = ["Electronics", "Fashion", "Home", "Sports", "Books"]
        for pid in range(1, 1001):
            cat = categories[pid % len(categories)]
            base_price = random.uniform(50, 1000)
            discount = random.choices([0, 0.05, 0.10, 0.15, 0.20], weights=[60, 20, 10, 7, 3], k=1)[0]
            
            catalog[pid] = {
                "product_id": pid,
                "name": f"Product #{pid}",
                "category": cat,
                "price": round(base_price, 2),
                "discount": discount,
                "final_price": round(base_price * (1 - discount), 2),
                "rating": round(random.uniform(3.5, 5.0), 1),
                "in_stock": random.random() < 0.95
            }
        return catalog
    
    def _index_by_category(self):
        """Index products by category_id for weighted selection"""
        by_cat = {}
        for prod in self.products.values():
            # Use category_id if available, otherwise use category name
            cat_key = prod.get("category_id") or prod["category"]
            if cat_key not in by_cat:
                by_cat[cat_key] = []
            by_cat[cat_key].append(prod)
        return by_cat
    
    def get(self, pid):
        return self.products.get(pid)
    
    def random_product(self, prefer_popular_categories=True):
        """Get random product with optional category popularity weighting"""
        if prefer_popular_categories and self.categories_data:
            # Select category based on popularity weights
            category_ids = [cid for cid in CATEGORY_WEIGHTS.keys() if cid in self.by_category]
            if category_ids:
                weights = [CATEGORY_WEIGHTS.get(cid, 1) for cid in category_ids]
                category_id = random.choices(category_ids, weights=weights, k=1)[0]
                return random.choice(self.by_category[category_id])
        
        # Fallback to random selection
        return random.choice(list(self.products.values()))


# ==================== SESSION GENERATOR ====================

class SessionGenerator:
    """Generates realistic user journey with logical flow"""
    def __init__(self, catalog, payment_methods=None):
        self.catalog = catalog
        self.payment_methods = payment_methods
        # Get list of category names for search terms
        if catalog.categories_data:
            self.category_names = [cat['name'] for cat in catalog.categories_data.values()]
        else:
            self.category_names = list(set(p['category'] for p in catalog.products.values()))
    
    def generate_actions(self, user_segment, device_type, is_anonymous=False):
        """Generate session actions based on user behavior"""
        actions = []
        cart_items = []
        page = "/"
        
        # Session characteristics by segment
        if user_segment == "vip":
            num_actions = random.randint(8, 20)
            conversion_rate = 0.7
        elif user_segment == "loyal":
            num_actions = random.randint(5, 15)
            conversion_rate = 0.5
        elif user_segment == "returning":
            num_actions = random.randint(3, 10)
            conversion_rate = 0.3
        else:  # new
            num_actions = random.randint(2, 8)
            conversion_rate = 0.15
        
        # Anonymous users cannot purchase (must register/login first)
        if is_anonymous:
            conversion_rate = 0.0
        
        time_offset = 0
        
        for step in range(num_actions):
            time_offset += random.randint(5, 60)
        
            # First action
            if step == 0:
                if random.random() < 0.4:
                    search_term = random.choice(self.category_names).lower()
                    actions.append({
                        "type": "search",
                        "page": "/search",
                        "search_term": search_term,
                        "results_count": random.randint(5, 50),
                        "time_offset": time_offset
                    })
                    page = "/search"
                else:
                    page = "/" if random.random() < 0.5 else f"/category/{random.choice(self.category_names).lower().replace(' ', '-')}"
                    actions.append({"type": "view", "page": page, "time_offset": time_offset})
            
            # Navigate to product
            elif page in ["/", "/search"] or "/category/" in page:
                product = self.catalog.random_product()
                page = f"/product/{product['product_id']}"
                actions.append({
                    "type": "view",
                    "page": page,
                    "product_id": product["product_id"],
                    "product_name": product["name"],
                    "category": product["category"],
                    "price": product["final_price"],
                    "time_offset": time_offset
                })
            
            # On product page
            elif "/product/" in page:
                action_type = random.choices(
                    ["add_to_cart", "wishlist", "view_another", "review"],
                    weights=[40, 10, 35, 15]
                )[0]
                
                if action_type == "add_to_cart":
                    pid = int(page.split("/")[-1])
                    product = self.catalog.get(pid)
                    if product:
                        qty = random.randint(1, 3)
                        cart_items.append({"product": product, "quantity": qty})
                        actions.append({
                            "type": "add_to_cart",
                            "page": page,
                            "product_id": product["product_id"],
                            "product_name": product["name"],
                            "quantity": qty,
                            "price": product["final_price"],
                            "time_offset": time_offset
                        })
                        if random.random() < 0.4 or len(cart_items) >= 3:
                            page = "/cart"
                
                elif action_type == "wishlist":
                    pid = int(page.split("/")[-1])
                    product = self.catalog.get(pid)
                    if product:
                        actions.append({
                            "type": "wishlist",
                            "page": page,
                            "product_id": product["product_id"],
                            "time_offset": time_offset
                        })
                
                elif action_type == "review":
                    pid = int(page.split("/")[-1])
                    actions.append({
                        "type": "review",
                        "page": page,
                        "product_id": pid,
                        "rating": random.randint(3, 5),
                        "time_offset": time_offset
                    })
                
                else:  # view_another
                    product = self.catalog.random_product()
                    page = f"/product/{product['product_id']}"
                    actions.append({
                        "type": "view",
                        "page": page,
                        "product_id": product["product_id"],
                        "product_name": product["name"],
                        "category": product["category"],
                        "price": product["final_price"],
                        "time_offset": time_offset
                    })
            
            # On cart page
            elif page == "/cart":
                if cart_items and random.random() < conversion_rate:
                    # Checkout and purchase
                    page = "/checkout"
                    actions.append({"type": "view", "page": "/checkout", "time_offset": time_offset})
                    
                    total = sum(item["product"]["final_price"] * item["quantity"] for item in cart_items)
                    
                    # Select payment method from database or use default
                    if self.payment_methods:
                        payment_method = random.choice(self.payment_methods)['display_name'].lower().replace(' ', '_')
                    else:
                        payment_method = random.choice(["credit_card", "paypal", "bank_transfer", "cod"])
                    
                    actions.append({
                        "type": "purchase",
                        "page": "/checkout",
                        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
                        "items": [
                            {
                                "product_id": item["product"]["product_id"],
                                "product_name": item["product"]["name"],
                                "quantity": item["quantity"],
                                "price": item["product"]["final_price"]
                            }
                            for item in cart_items
                        ],
                        "total_amount": round(total, 2),
                        "payment_method": payment_method,
                        "time_offset": time_offset
                    })
                    break  # End after purchase
                
                elif cart_items and random.random() < 0.15:
                    # Remove item
                    removed = cart_items.pop(random.randint(0, len(cart_items) - 1))
                    actions.append({
                        "type": "remove_from_cart",
                        "page": "/cart",
                        "product_id": removed["product"]["product_id"],
                        "time_offset": time_offset
                    })
                else:
                    # Continue shopping
                    page = f"/category/{random.choice(self.category_names).lower().replace(' ', '-')}"
                    actions.append({"type": "view", "page": page, "time_offset": time_offset})
        
        return actions


# ==================== ORDER BULK INSERT ====================

class OrderManager:
    """Manages bulk insertion of orders and order items"""
    def __init__(self, connection, batch_size=500, write_to_db=True):
        self.connection = connection
        self.batch_size = batch_size
        self.write_to_db = write_to_db
        self.order_queue = deque()
        self.order_items_queue = deque()
        self.total_orders = 0
        self.total_items = 0
    
    def add_order(self, customer_id, order_date, total_amount, discount, payment_method_id, items):
        """Add order to queue for bulk insert"""
        order = {
            'customer_id': customer_id,
            'order_date': order_date,
            'total_amount': total_amount,
            'discount': discount,
            'total_final': total_amount * (1 - discount),
            'payment_method_id': payment_method_id,
            'items': items
        }
        self.order_queue.append(order)
        
        # Auto-flush if batch size reached
        if len(self.order_queue) >= self.batch_size:
            self.flush()
    
    def flush(self):
        """Bulk insert all queued orders and order items"""
        if not self.order_queue:
            return
        
        # If write_to_db is False, just count and clear queue
        if not self.write_to_db or not self.connection:
            self.total_orders += len(self.order_queue)
            total_items = sum(len(order['items']) for order in self.order_queue)
            self.total_items += total_items
            self.order_queue.clear()
            return
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare orders batch
            orders_data = []
            for order in self.order_queue:
                orders_data.append((
                    order['customer_id'],
                    order['order_date'],
                    order['total_amount'],
                    order['discount'],
                    order['total_final'],
                    order['payment_method_id']
                ))
            
            # Bulk insert orders
            insert_order_sql = """
                INSERT INTO orders (customer_id, order_date, total_amount, discount, total_final, payment_method_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_order_sql, orders_data)
            
            # Get the starting order_id for this batch
            cursor.execute("SELECT LAST_INSERT_ID()")
            first_order_id = cursor.fetchone()[0]
            
            # Prepare order items batch
            order_items_data = []
            order_id = first_order_id - len(orders_data)  # Start from first inserted order
            for order in self.order_queue:
                order_id += 1
                for item in order['items']:
                    order_items_data.append((
                        order_id,
                        item['product_id'],
                        item['quantity'],
                        item['price']
                    ))
            
            # Bulk insert order items
            if order_items_data:
                insert_items_sql = """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.executemany(insert_items_sql, order_items_data)
            
            self.connection.commit()
            
            self.total_orders += len(self.order_queue)
            self.total_items += len(order_items_data)
            
            # Clear queues
            self.order_queue.clear()
            cursor.close()
            
        except Error as e:
            print(f"⚠️  Error bulk inserting orders: {e}")
            self.connection.rollback()
    
    def get_stats(self):
        """Return statistics about inserted orders"""
        return {
            'total_orders': self.total_orders,
            'total_items': self.total_items,
            'pending_orders': len(self.order_queue)
        }

# ==================== EVENT GENERATION ====================

def gen_event(catalog, session_gen, user_id_range=(1, 100000), base_time=None, customer_ids=None, payment_methods=None, order_manager=None):
    """Generate comprehensive event with weighted distributions"""
    if base_time is None:
        base_time = datetime.datetime.utcnow()
    
    # User with anonymous probability (25% anonymous)
    is_anonymous = random.random() < 0.25
    
    # Use real customer_id from database if available
    if is_anonymous:
        user_id = None
    elif customer_ids:
        user_id = random.choice(customer_ids)
    else:
        user_id = random.randint(user_id_range[0], user_id_range[1])
    
    # User segment based on user_id with realistic distribution
    if user_id:
        segment_rand = user_id % 100
        if segment_rand < 5:
            user_segment = "vip"          # 5% VIP
        elif segment_rand < 20:
            user_segment = "loyal"        # 15% loyal
        elif segment_rand < 50:
            user_segment = "returning"    # 30% returning
        else:
            user_segment = "new"          # 50% new
    else:
        user_segment = "new"
    
    # Session
    session_id = f"sess-{uuid.uuid4().hex[:16]}"
    
    # Weighted device selection
    device = weighted_random_choice(DEVICES)
    
    # Weighted city selection (population-based)
    location = weighted_random_choice(CITIES)
    
    # Weighted referrer selection
    referrer = weighted_random_choice(REFERRERS)
    
    # Weighted source selection
    source = weighted_random_choice(SOURCES)
    source_name = source["name"]
    
    # Weighted campaign selection (50% no campaign)
    campaign_obj = weighted_random_choice(CAMPAIGNS)
    campaign = campaign_obj["name"]
    
    # Generate actions
    actions = session_gen.generate_actions(user_segment, device["device_type"], is_anonymous)
    
    # Process purchases - insert to database via order manager
    for action in actions:
        if action["type"] == "purchase" and order_manager and user_id:
            # Map payment method string to database ID
            payment_method_str = action.get("payment_method", "credit_card")
            payment_method_id = 1  # Default
            
            if payment_methods:
                # Find matching payment method ID
                for pm in payment_methods:
                    if payment_method_str.lower() in pm['display_name'].lower():
                        payment_method_id = pm['payment_method_id']
                        break
            
            # Calculate order totals
            items = action.get("items", [])
            total_amount = action.get("total_amount", 0)
            discount = random.choice([0, 0.05, 0.10, 0.15])  # Order-level discount
            order_date = base_time.date()
            
            # Add to bulk insert queue
            order_manager.add_order(
                customer_id=user_id,
                order_date=order_date,
                total_amount=total_amount,
                discount=discount,
                payment_method_id=payment_method_id,
                items=items
            )
    
    # Metrics
    duration = actions[-1]["time_offset"] if actions else 0
    page_views = sum(1 for a in actions if a["type"] == "view")
    has_purchase = any(a["type"] == "purchase" for a in actions)
    revenue = sum(a.get("total_amount", 0) for a in actions if a["type"] == "purchase")
    
    return {
        "event_id": f"evt-{uuid.uuid4().hex[:16]}",
        "timestamp": base_time.isoformat() + "Z",
        "user_id": user_id,
        "user_segment": user_segment,
        "session_id": session_id,
        "device": {
            "type": device["device_type"],
            "os": device["os"],
            "browser": device["browser"],
            "version": device["version"]
        },
        "location": {
            "city": location["name"],
            "country": location["country"],
            "coordinates": {"lat": location["lat"], "lon": location["lon"]}
        },
        "referrer": referrer["url"],
        "referrer_type": referrer["type"],
        "source": source_name,
        "campaign": campaign,
        "actions": actions,
        "session_metrics": {
            "duration_seconds": duration,
            "page_views": page_views,
            "actions_count": len(actions),
            "has_purchase": has_purchase,
            "revenue": round(revenue, 2)
        },
        "properties": {
            "ab_test": random.choice(["A", "B", "control"]),
            "language": random.choice(["vi-VN", "en-US"]),
            "currency": "VND",
            "is_mobile": device["device_type"] == "mobile"
        }
    }


def write_ndjson(path, n=1000, user_id_range=(1, 100000), catalog=None, session_gen=None, base_time=None, max_file_size_mb=100, 
                 db_connection=None, customer_ids=None, payment_methods=None, order_manager=None):
    """Write events to multiple NDJSON files, splitting at max_file_size_mb"""
    if catalog is None:
        print("Initializing product catalog...")
        catalog = ProductCatalog(db_connection=db_connection)
    
    if session_gen is None:
        session_gen = SessionGenerator(catalog, payment_methods)
    
    if base_time is None:
        base_time = datetime.datetime.utcnow()
    
    p = Path(path)
    output_dir = p.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating {n} events (max {max_file_size_mb}MB per file)...")
    
    # Use tqdm if available, otherwise fallback to manual progress
    if TQDM_AVAILABLE:
        iterator = tqdm(range(n), desc="Generating events", unit="events", 
                       ncols=100, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
    else:
        iterator = range(n)
    
    max_file_size_bytes = max_file_size_mb * 1024 * 1024
    part_number = 0
    current_file = None
    current_file_size = 0
    files_created = []
    
    def get_part_path(part_num):
        """Generate part file path"""
        base_name = p.stem if p.suffix else p.name
        return output_dir / f"{base_name}-part-{part_num:04d}.ndjson"
    
    try:
        for i in iterator:
            # Generate event
            time_offset = random.randint(0, 86400)  # Within 24 hours
            event_time = base_time + datetime.timedelta(seconds=time_offset)
            event = gen_event(catalog, session_gen, user_id_range, event_time, customer_ids, payment_methods, order_manager)
            event_json = json.dumps(event, ensure_ascii=False) + "\n"
            event_size = len(event_json.encode('utf-8'))
            
            # Check if we need to create a new file
            if current_file is None or current_file_size + event_size > max_file_size_bytes:
                # Close current file if exists
                if current_file is not None:
                    current_file.close()
                    if TQDM_AVAILABLE:
                        iterator.set_postfix({"files": len(files_created), "current_file_mb": f"{current_file_size/1024/1024:.1f}"})
                
                # Open new file
                part_path = get_part_path(part_number)
                files_created.append(part_path)
                current_file = part_path.open("w", encoding="utf-8")
                current_file_size = 0
                part_number += 1
            
            # Write event
            current_file.write(event_json)
            current_file_size += event_size
            
            # Fallback progress for when tqdm is not available
            if not TQDM_AVAILABLE and (i + 1) % 1000 == 0:
                print(f"  Progress: {i + 1}/{n} events ({(i+1)/n*100:.1f}%) | Files: {len(files_created)}")
    
    finally:
        # Close last file
        if current_file is not None:
            current_file.close()
        
        # Flush remaining orders
        if order_manager:
            order_manager.flush()
            stats = order_manager.get_stats()
            if stats['total_orders'] > 0:
                print(f"\n📦 Order Statistics:")
                print(f"   Total orders inserted: {stats['total_orders']}")
                print(f"   Total order items: {stats['total_items']}")
    
    print(f"✅ Successfully wrote {n} events to {len(files_created)} file(s)")
    for idx, file_path in enumerate(files_created, 1):
        file_size_mb = file_path.stat().st_size / 1024 / 1024
        print(f"   Part {idx}: {file_path.name} ({file_size_mb:.2f} MB)")
    
    return files_created


def generate_time_series(output_dir, days=7, events_per_day=10000, user_id_range=(1, 100000), max_file_size_mb=100):
    """Generate time-series data for multiple days
    
    Args:
        events_per_day: Can be int (fixed) or tuple (min, max) for random range
                        Example: 10000 or (8000, 12000)
    """
    # Parse events_per_day if it's a range
    if isinstance(events_per_day, tuple) and len(events_per_day) == 2:
        events_min, events_max = events_per_day
        events_display = f"{events_min}-{events_max}"
        total_estimate = f"~{days * ((events_min + events_max) // 2)}"
    else:
        events_min = events_max = events_per_day
        events_display = str(events_per_day)
        total_estimate = str(days * events_per_day)
    
    print(f"\n{'='*60}")
    print(f"Time-Series Data Generation")
    print(f"{'='*60}")
    print(f"Days: {days} | Events/day: {events_display} | Total: {total_estimate}")
    print(f"User ID range: {user_id_range[0]}-{user_id_range[1]}")
    print(f"Max file size: {max_file_size_mb}MB")
    print(f"{'='*60}\n")
    
    # Initialize database connection
    db_connection = create_db_connection()
    write_to_db = not args.no_write_db
    customer_ids = load_customers_from_db(db_connection) if db_connection else None
    payment_methods = load_payment_methods_from_db(db_connection) if db_connection else None
    order_manager = OrderManager(db_connection, ORDER_BATCH_SIZE, write_to_db=write_to_db) if db_connection else None
    
    if args.no_write_db:
        print("⚠️  --no-write-db enabled: Orders will NOT be written to MySQL")
    
    catalog = ProductCatalog(db_connection=db_connection)
    session_gen = SessionGenerator(catalog, payment_methods)
    
    for day_offset in range(days):
        date = datetime.date.today() - datetime.timedelta(days=days - day_offset - 1)
        date_str = date.isoformat()
        start_time = datetime.datetime.combine(date, datetime.time(0, 0, 0))
        
        # Randomly select event count within range
        daily_events = random.randint(events_min, events_max)
        
        outdir = Path(output_dir) / f"ingest_date={date_str}"
        outpath = outdir / "part"
        
        print(f"\n📅 Generating {date_str} ({daily_events} events)...")
        write_ndjson(outpath, daily_events, user_id_range, catalog, session_gen, start_time, max_file_size_mb,
                    db_connection, customer_ids, payment_methods, order_manager)
    
    # Final flush and statistics
    if order_manager:
        order_manager.flush()
        stats = order_manager.get_stats()
        print(f"\n📦 Total Order Statistics:")
        print(f"   Orders: {stats['total_orders']} | Items: {stats['total_items']}")
    
    # Close database connection
    if db_connection and db_connection.is_connected():
        db_connection.close()
        print("✅ Database connection closed")
    
    print(f"\n{'='*60}")
    print(f"✅ Time-series generation complete!")
    print(f"📁 Output: {output_dir}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enhanced E-commerce Clickstream Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1000 events
  python generator.py --n 1000
  
  # Generate 50000 events for specific date
  python generator.py --n 50000 --date 2025-11-10
  
  # Generate 7 days of data (fixed 10k events/day)
  python generator.py --time_series --days 7 --events_per_day 10000
  
  # Generate 7 days with random events (8k-12k per day)
  python generator.py --time_series --days 7 --events_per_day 8000-12000
  
  # Custom user ID range
  python generator.py --n 5000 --user_id_start 1 --user_id_end 50000
        """
    )
    
    parser.add_argument("--out_dir", default="bronze/user_activity", help="Output directory")
    parser.add_argument("--n", type=int, default=1000, help="Number of events")
    parser.add_argument("--date", default=None, help="Date (YYYY-MM-DD, default: today)")
    parser.add_argument("--time_series", action="store_true", help="Generate time-series data")
    parser.add_argument("--days", type=int, default=7, help="Days for time-series")
    parser.add_argument("--events_per_day", type=str, default="10000", help="Events per day (single number or range like '8000-12000')")
    parser.add_argument("--user_id_start", type=int, default=1, help="User ID range start")
    parser.add_argument("--user_id_end", type=int, default=100000, help="User ID range end")
    parser.add_argument("--max_file_size_mb", type=int, default=100, help="Maximum file size in MB (default: 100)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--no-write-db", action="store_true", help="Don't write orders to MySQL (still read from MySQL)")
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
        print(f"🎲 Random seed: {args.seed}")
    
    user_id_range = (args.user_id_start, args.user_id_end)
    
    # Parse events_per_day (can be '10000' or '8000-12000')
    if '-' in args.events_per_day:
        event_parts = args.events_per_day.split('-')
        events_per_day = (int(event_parts[0]), int(event_parts[1]))
    else:
        events_per_day = int(args.events_per_day)
    
    if args.time_series:
        generate_time_series(args.out_dir, args.days, events_per_day, user_id_range, args.max_file_size_mb)
    else:
        date = args.date or datetime.date.today().isoformat()
        outdir = Path(args.out_dir) / f"ingest_date={date}"
        outpath = outdir / "part"
        
        start = datetime.datetime.now()
        
        # Initialize database connection
        db_connection = create_db_connection()
        write_to_db = not args.no_write_db
        customer_ids = load_customers_from_db(db_connection) if db_connection else None
        payment_methods = load_payment_methods_from_db(db_connection) if db_connection else None
        order_manager = OrderManager(db_connection, ORDER_BATCH_SIZE, write_to_db=write_to_db) if db_connection else None
        
        if args.no_write_db:
            print("⚠️  --no-write-db enabled: Orders will NOT be written to MySQL")
        
        catalog = ProductCatalog(db_connection=db_connection)
        session_gen = SessionGenerator(catalog, payment_methods)
        
        # For single date mode, use args.n (not events_per_day)
        files = write_ndjson(outpath, args.n, user_id_range, catalog, session_gen, max_file_size_mb=args.max_file_size_mb,
                           db_connection=db_connection, customer_ids=customer_ids, 
                           payment_methods=payment_methods, order_manager=order_manager)
        
        # Close database connection
        if db_connection and db_connection.is_connected():
            db_connection.close()
            print("✅ Database connection closed")
        
        elapsed = (datetime.datetime.now() - start).total_seconds()
        print(f"\n📊 Performance: {elapsed:.2f}s ({args.n / elapsed:.0f} events/sec)")
        print(f"📁 Output directory: {outdir}")
