#!/usr/bin/env python3
"""
Order Data Generator for E-commerce Lakehouse
Generates realistic order and order_item data in CSV format
Features:
- Multi-threaded generation (1 thread per month)
- Random order count per month within specified range
- Loads real customer_id and product_id from MySQL
- Progress tracking with tqdm
- Outputs: order_YYYYMMDD.csv (YYYYMMDD = first day of NEXT month)
  Example: order_20251201.csv contains data for November 2025
"""

import csv
import random
import argparse
import datetime
from pathlib import Path
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

try:
    import mysql.connector
    from mysql.connector import Error
    MYSQL_AVAILABLE = True
except ImportError:
    print("⚠️  mysql-connector-python not installed. Install with: pip install mysql-connector-python")
    MYSQL_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    print("⚠️  tqdm not installed. Install with: pip install tqdm")
    TQDM_AVAILABLE = False

# ==================== DATABASE CONFIGURATION ====================

DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': 'oltp',
    'user': 'root',
    'password': 'root',
    'charset': 'utf8mb4',
    'use_unicode': True
}

# ==================== BUSINESS RULES ====================

# Order value distribution (VND)
ORDER_VALUE_RANGES = [
    {"min": 50000, "max": 200000, "weight": 30},      # Small orders
    {"min": 200000, "max": 500000, "weight": 35},     # Medium orders
    {"min": 500000, "max": 1000000, "weight": 20},    # Large orders
    {"min": 1000000, "max": 3000000, "weight": 10},   # Very large
    {"min": 3000000, "max": 10000000, "weight": 5}    # Premium
]

# Items per order distribution
ITEMS_PER_ORDER = [
    {"count": 1, "weight": 40},
    {"count": 2, "weight": 30},
    {"count": 3, "weight": 15},
    {"count": 4, "weight": 8},
    {"count": 5, "weight": 4},
    {"count": range(6, 11), "weight": 3}  # 6-10 items
]

# Discount distribution
DISCOUNT_DISTRIBUTION = [
    {"value": 0.0, "weight": 50},
    {"value": 0.05, "weight": 20},
    {"value": 0.10, "weight": 15},
    {"value": 0.15, "weight": 8},
    {"value": 0.20, "weight": 5},
    {"value": 0.25, "weight": 2}
]

# Payment method weights - realistic distribution
# COD (1) most popular, e-wallets (2,3) growing, cards (4,5) less common
PAYMENT_METHOD_WEIGHTS = [
    {"id": 1, "weight": 45},  # COD - most popular in Vietnam
    {"id": 2, "weight": 25},  # MoMo/ZaloPay - popular e-wallet
    {"id": 3, "weight": 15},  # Bank transfer
    {"id": 4, "weight": 10},  # Credit card
    {"id": 5, "weight": 5}    # Debit card
]

# Product popularity tiers - some products much more popular
PRODUCT_POPULARITY = {
    'hot': {'weight': 50, 'pct': 0.10},      # Top 10% products get 50% of sales
    'popular': {'weight': 30, 'pct': 0.20},  # Next 20% get 30% of sales  
    'normal': {'weight': 15, 'pct': 0.30},   # Next 30% get 15% of sales
    'niche': {'weight': 5, 'pct': 0.40}      # Remaining 40% get 5% of sales
}

# Default payment methods if DB not available
DEFAULT_PAYMENT_METHODS = [1, 2, 3, 4, 5]

# ==================== DATABASE FUNCTIONS ====================

def create_db_connection():
    """Create MySQL database connection"""
    if not MYSQL_AVAILABLE:
        print("❌ MySQL connector not available")
        return None
    
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print(f"✅ Connected to MySQL database: {DB_CONFIG['database']}")
            return connection
    except Error as e:
        print(f"❌ Database connection error: {e}")
        return None

def load_customer_ids(connection):
    """Load all customer IDs from database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"✅ Loaded {len(customer_ids)} customer IDs")
        return customer_ids
    except Error as e:
        print(f"❌ Error loading customers: {e}")
        return None

def load_products(connection):
    """Load product information from database"""
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT product_id, product_name, price, category_id, brand_id
            FROM products
        """)
        products = {}
        for row in cursor.fetchall():
            products[row['product_id']] = {
                'product_id': row['product_id'],
                'name': row['product_name'],
                'price': float(row['price']),
                'category_id': row['category_id'],
                'brand_id': row['brand_id']
            }
        cursor.close()
        print(f"✅ Loaded {len(products)} products")
        return products
    except Error as e:
        print(f"❌ Error loading products: {e}")
        return None

def load_payment_methods(connection):
    """Load payment method IDs from database"""
    if not connection:
        return DEFAULT_PAYMENT_METHODS
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT payment_method_id FROM payment_methods")
        payment_method_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"✅ Loaded {len(payment_method_ids)} payment methods")
        return payment_method_ids
    except Error as e:
        print(f"⚠️  Error loading payment methods: {e}, using defaults")
        return DEFAULT_PAYMENT_METHODS

def write_orders_to_mysql(connection, orders, order_items, batch_size=100):
    """Write orders and order items to MySQL database in batches"""
    if not connection:
        print("❌ No database connection")
        return False
    
    try:
        cursor = connection.cursor()
        
        # Insert orders in batches
        orders_sql = """
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, 
                           payment_method_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        print(f"  ↳ Inserting {len(orders):,} orders...")
        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]
            order_values = [
                (o['order_id'], o['customer_id'], o['order_date'], o['total_amount'],
                 o['payment_method_id'], o['created_at'], o['updated_at'])
                for o in batch
            ]
            cursor.executemany(orders_sql, order_values)
            connection.commit()
            
            if TQDM_AVAILABLE:
                print(f"    → {min(i + batch_size, len(orders)):,}/{len(orders):,} orders")
        
        # Insert order items in batches
        items_sql = """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, price, discount)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        print(f"  ↳ Inserting {len(order_items):,} order items...")
        for i in range(0, len(order_items), batch_size):
            batch = order_items[i:i + batch_size]
            item_values = [
                (item['order_item_id'], item['order_id'], item['product_id'],
                 item['quantity'], item['price'], item['discount'])
                for item in batch
            ]
            cursor.executemany(items_sql, item_values)
            connection.commit()
            
            if TQDM_AVAILABLE:
                print(f"    → {min(i + batch_size, len(order_items)):,}/{len(order_items):,} items")
        
        cursor.close()
        print(f"✅ Successfully wrote to MySQL")
        return True
        
    except Error as e:
        print(f"❌ Database write error: {e}")
        connection.rollback()
        return False

# ==================== HELPER FUNCTIONS ====================

def weighted_random_choice(items, weight_key='weight'):
    """Select item based on weights"""
    if not items:
        return None
    
    weights = [item[weight_key] for item in items]
    return random.choices(items, weights=weights, k=1)[0]

def generate_order_time(month_date, day_of_month):
    """Generate realistic order timestamp for a specific day in month"""
    # Peak hours: 9-11am, 2-4pm, 7-10pm
    hour_weights = {
        0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2, 6: 3, 7: 5, 8: 8,
        9: 12, 10: 15, 11: 12, 12: 10, 13: 8, 14: 10, 15: 12,
        16: 10, 17: 8, 18: 6, 19: 10, 20: 15, 21: 12, 22: 8, 23: 4
    }
    
    hours, weights = zip(*hour_weights.items())
    hour = random.choices(hours, weights=weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    # Create date for specific day in month
    order_date = datetime.date(month_date.year, month_date.month, day_of_month)
    
    return datetime.datetime.combine(
        order_date,
        datetime.time(hour, minute, second)
    )

# ==================== ORDER GENERATION ====================

class OrderGenerator:
    """Generates realistic orders and order items"""
    
    def __init__(self, customer_ids, products, payment_methods, start_order_id=1, start_order_item_id=1):
        self.customer_ids = customer_ids or list(range(1, 10001))
        self.products = products or {}
        self.product_ids = list(self.products.keys()) if products else list(range(1, 1001))
        
        # Create weighted product distribution (some products more popular)
        self.weighted_products = self._create_weighted_products()
        
        # Create weighted payment method list
        if payment_methods:
            # Use provided payment methods with weights
            self.payment_method_choices = []
            for pm in PAYMENT_METHOD_WEIGHTS:
                if pm['id'] in payment_methods:
                    self.payment_method_choices.append(pm)
        else:
            self.payment_method_choices = PAYMENT_METHOD_WEIGHTS
        
        # For thread-safe order_id generation
        self.order_id_lock = Lock()
        self.current_order_id = start_order_id
        
        # For thread-safe order_item_id generation
        self.order_item_id_lock = Lock()
        self.current_order_item_id = start_order_item_id
    
    def _create_weighted_products(self):
        """Create weighted product list based on popularity tiers"""
        if not self.product_ids:
            return []
        
        # Shuffle to randomize which products are in which tier
        shuffled = self.product_ids.copy()
        random.shuffle(shuffled)
        
        weighted = []
        total = len(shuffled)
        
        # Assign products to tiers
        tiers = [
            ('hot', int(total * PRODUCT_POPULARITY['hot']['pct']), PRODUCT_POPULARITY['hot']['weight']),
            ('popular', int(total * PRODUCT_POPULARITY['popular']['pct']), PRODUCT_POPULARITY['popular']['weight']),
            ('normal', int(total * PRODUCT_POPULARITY['normal']['pct']), PRODUCT_POPULARITY['normal']['weight']),
            ('niche', total, PRODUCT_POPULARITY['niche']['weight'])  # Remaining products
        ]
        
        idx = 0
        for tier_name, tier_end, weight in tiers:
            tier_count = min(tier_end, total) - idx
            for _ in range(tier_count):
                if idx < len(shuffled):
                    weighted.append({'id': shuffled[idx], 'weight': weight})
                    idx += 1
        
        return weighted
    
    def get_next_order_id(self):
        """Thread-safe order ID generation"""
        with self.order_id_lock:
            order_id = self.current_order_id
            self.current_order_id += 1
            return order_id
    
    def allocate_order_id_range(self, count):
        """Allocate a range of order IDs for batch processing"""
        with self.order_id_lock:
            start_id = self.current_order_id
            self.current_order_id += count
            return start_id
    
    def get_next_order_item_id(self):
        """Thread-safe order item ID generation"""
        with self.order_item_id_lock:
            order_item_id = self.current_order_item_id
            self.current_order_item_id += 1
            return order_item_id
    
    def allocate_order_item_id_range(self, count):
        """Allocate a range of order item IDs for batch processing"""
        with self.order_item_id_lock:
            start_id = self.current_order_item_id
            self.current_order_item_id += count
            return start_id
    
    def generate_order_items(self, order_id, target_amount):
        """Generate order items that sum up to approximately target_amount"""
        # Determine number of items
        items_config = weighted_random_choice(ITEMS_PER_ORDER)
        num_items = items_config['count']
        if isinstance(num_items, range):
            num_items = random.choice(list(num_items))
        
        items = []
        total = 0.0
        
        # Select products using weighted random (popular products chosen more often)
        selected_products = []
        if self.weighted_products:
            # Weighted selection - popular products appear more frequently
            product_pool = random.choices(
                [p['id'] for p in self.weighted_products],
                weights=[p['weight'] for p in self.weighted_products],
                k=num_items * 3  # Generate more to ensure uniqueness
            )
            # Remove duplicates while preserving selection bias
            seen = set()
            for pid in product_pool:
                if pid not in seen:
                    selected_products.append(pid)
                    seen.add(pid)
                    if len(selected_products) >= num_items:
                        break
        else:
            selected_products = random.sample(self.product_ids, min(num_items, len(self.product_ids)))
        
        for i, product_id in enumerate(selected_products):
            # Get product unit_price from database
            if self.products and product_id in self.products:
                unit_price = self.products[product_id]['price']
            else:
                unit_price = random.uniform(50000, 500000)
            
            # Determine quantity (last item adjusts to meet target)
            if i == len(selected_products) - 1:
                remaining = target_amount - total
                if remaining > 0:
                    quantity = max(1, int(remaining / unit_price))
                else:
                    quantity = 1
            else:
                quantity = random.randint(1, 5)
            
            # Apply item-level discount
            discount = weighted_random_choice(DISCOUNT_DISTRIBUTION)['value']
            
            # Calculate item total (price always equals unit_price from DB)
            item_total = unit_price * quantity
            total += item_total
            
            items.append({
                'order_item_id': self.get_next_order_item_id(),
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'price': round(unit_price, 2),  # Always use unit_price from products table
                'discount': discount
            })
        
        return items, total
    
    def generate_order(self, month_date, day_of_month):
        """Generate single order with items for a specific day in month"""
        order_id = self.get_next_order_id()
        customer_id = random.choice(self.customer_ids)
        
        # Use weighted payment method selection
        payment_method = weighted_random_choice(self.payment_method_choices, weight_key='weight')
        payment_method_id = payment_method['id']
        
        # Determine order value
        value_range = weighted_random_choice(ORDER_VALUE_RANGES)
        target_amount = random.uniform(value_range['min'], value_range['max'])
        
        # Generate order items
        items, total_amount = self.generate_order_items(order_id, target_amount)
        
        # Order timestamp
        created_at = generate_order_time(month_date, day_of_month)
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': created_at.date().strftime('%Y-%m-%d'),
            'total_amount': round(total_amount, 2),
            'payment_method_id': payment_method_id,
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': created_at.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return order, items

# ==================== CSV WRITING ====================

def write_orders_csv(orders, filepath):
    """Write orders to CSV file without header"""
    fieldnames = ['order_id', 'customer_id', 'order_date', 'total_amount', 
                  'payment_method_id', 'created_at', 'updated_at']
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # No header
        writer.writerows(orders)

def write_order_items_csv(order_items, filepath):
    """Write order items to CSV file without header"""
    fieldnames = ['order_item_id', 'order_id', 'product_id', 'quantity', 'price', 'discount']
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # No header
        writer.writerows(order_items)

# ==================== MONTH GENERATION ====================

def generate_month_data(generator, month_date, num_orders, output_dir, show_progress=False, write_to_db=False, db_connection=None):
    """Generate data for a single month (thread worker function)"""
    import calendar
    
    month_str = month_date.strftime('%Y-%m')
    
    # Pre-allocate ID ranges to minimize lock contention
    # Estimate: average 2.5 items per order
    estimated_items = int(num_orders * 2.5)
    start_order_id = generator.allocate_order_id_range(num_orders)
    start_item_id = generator.allocate_order_item_id_range(estimated_items)
    
    if show_progress and TQDM_AVAILABLE:
        print(f"\n🔄 {month_str}: Generating {num_orders:,} orders...")
    
    # Get number of days in this month
    _, days_in_month = calendar.monthrange(month_date.year, month_date.month)
    
    # Calculate next month for file naming
    if month_date.month == 12:
        next_month = datetime.date(month_date.year + 1, 1, 1)
    else:
        next_month = datetime.date(month_date.year, month_date.month + 1, 1)
    
    file_date_str = next_month.strftime('%Y%m%d')
    
    orders = []
    all_items = []
    
    # Distribute orders across days in the month
    day_weights = []
    for day in range(1, days_in_month + 1):
        if day <= 5:
            weight = 0.8
        elif day <= 15:
            weight = 1.0
        elif day <= 25:
            weight = 1.2
        else:
            weight = 1.5
        day_weights.append(weight)
    
    days = list(range(1, days_in_month + 1))
    order_days = random.choices(days, weights=day_weights, k=num_orders)
    
    # Generate orders with progress tracking
    order_id_counter = start_order_id
    item_id_counter = start_item_id
    
    iterator = enumerate(order_days)
    if show_progress and TQDM_AVAILABLE:
        iterator = tqdm(iterator, total=num_orders, desc=f"{month_str}", leave=False)
    
    for idx, day in iterator:
        # Generate order with pre-allocated IDs
        customer_id = random.choice(generator.customer_ids)
        payment_method = weighted_random_choice(generator.payment_method_choices, weight_key='weight')
        payment_method_id = payment_method['id']
        
        value_range = weighted_random_choice(ORDER_VALUE_RANGES)
        target_amount = random.uniform(value_range['min'], value_range['max'])
        
        # Generate items
        items_config = weighted_random_choice(ITEMS_PER_ORDER)
        num_items = items_config['count']
        if isinstance(num_items, range):
            num_items = random.choice(list(num_items))
        
        # Select products
        selected_products = []
        if generator.weighted_products:
            product_pool = random.choices(
                [p['id'] for p in generator.weighted_products],
                weights=[p['weight'] for p in generator.weighted_products],
                k=num_items * 3
            )
            seen = set()
            for pid in product_pool:
                if pid not in seen:
                    selected_products.append(pid)
                    seen.add(pid)
                    if len(selected_products) >= num_items:
                        break
        else:
            selected_products = random.sample(generator.product_ids, min(num_items, len(generator.product_ids)))
        
        items = []
        total = 0.0
        for i, product_id in enumerate(selected_products):
            if generator.products and product_id in generator.products:
                unit_price = generator.products[product_id]['price']
            else:
                unit_price = random.uniform(50000, 500000)
            
            if i == len(selected_products) - 1:
                remaining = target_amount - total
                quantity = max(1, int(remaining / unit_price)) if remaining > 0 else 1
            else:
                quantity = random.randint(1, 5)
            
            discount = weighted_random_choice(DISCOUNT_DISTRIBUTION)['value']
            item_total = unit_price * quantity
            total += item_total
            
            items.append({
                'order_item_id': item_id_counter,
                'order_id': order_id_counter,
                'product_id': product_id,
                'quantity': quantity,
                'price': round(unit_price, 2),
                'discount': discount
            })
            item_id_counter += 1
        
        created_at = generate_order_time(month_date, day)
        
        order = {
            'order_id': order_id_counter,
            'customer_id': customer_id,
            'order_date': created_at.date().strftime('%Y-%m-%d'),
            'total_amount': round(total, 2),
            'payment_method_id': payment_method_id,
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': created_at.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        orders.append((order, items, created_at))
        order_id_counter += 1
    
    # Sort by timestamp
    if show_progress and TQDM_AVAILABLE:
        print(f"  ↳ Sorting {len(orders):,} orders...")
    orders.sort(key=lambda x: x[2])
    
    # Extract sorted orders and items
    final_orders = [o[0] for o in orders]
    final_items = [item for o in orders for item in o[1]]
    
    # Write to database if requested
    if write_to_db and db_connection:
        if show_progress and TQDM_AVAILABLE:
            print(f"  ↳ Writing to MySQL database...")
        write_orders_to_mysql(db_connection, final_orders, final_items)
    
    # Write files - split into 10 numbered files
    if show_progress and TQDM_AVAILABLE:
        print(f"  ↳ Writing CSV files (10 batches)...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Calculate batch size for 10 files
    total_orders = len(final_orders)
    batch_size = (total_orders + 9) // 10  # Ceiling division
    
    written_files = []
    
    for batch_num in range(1, 11):
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(start_idx + batch_size, total_orders)
        
        if start_idx >= total_orders:
            break
        
        # Get orders batch
        batch_orders = final_orders[start_idx:end_idx]
        
        # Get corresponding items for this order batch
        order_ids_in_batch = {o['order_id'] for o in batch_orders}
        batch_items = [item for item in final_items if item['order_id'] in order_ids_in_batch]
        
        # Write batch files
        orders_file = output_dir / f"orders_{file_date_str}_{batch_num}.csv"
        items_file = output_dir / f"order_items_{file_date_str}_{batch_num}.csv"
        
        write_orders_csv(batch_orders, orders_file)
        write_order_items_csv(batch_items, items_file)
        
        written_files.append({
            'batch': batch_num,
            'orders_file': orders_file,
            'items_file': items_file,
            'num_orders': len(batch_orders),
            'num_items': len(batch_items)
        })
    
    return {
        'month': month_date,
        'month_str': month_str,
        'file_date_str': file_date_str,
        'num_orders': len(final_orders),
        'num_items': len(final_items),
        'batches': written_files
    }

# ==================== MAIN GENERATION ====================

def generate_orders_multi_threaded(
    output_dir,
    months=6,
    orders_per_month_range=(1000, 5000),
    max_workers=4,
    customer_ids=None,
    products=None,
    payment_methods=None,
    start_month=None,
    growth_rate_range=(0.02, 0.04),  # 2-4% monthly growth
    write_to_db=False,
    db_connection=None,
    start_order_id=1,
    start_order_item_id=1
):
    """
    Generate orders using multiple threads (1 thread per month)
    Order counts grow progressively each month by random 2-4%
    
    Args:
        output_dir: Output directory for CSV files
        months: Number of months to generate
        orders_per_month_range: Tuple (min, max) for FIRST month's order count
        max_workers: Number of concurrent threads (set to 1 if write_to_db=True)
        customer_ids: List of customer IDs from database
        products: Dict of products from database
        payment_methods: List of payment method IDs
        start_month: Starting month as datetime.date (default: 6 months ago)
        growth_rate_range: Tuple (min, max) for monthly growth rate (default: 2-4%)
        write_to_db: Write data to MySQL database
        db_connection: MySQL connection object
        start_order_id: Starting order_id (default: 1)
        start_order_item_id: Starting order_item_id (default: 1)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Force single thread when writing to DB
    if write_to_db:
        max_workers = 1
        print("⚠️  Using single thread for MySQL write mode")
    
    if start_month is None:
        # Default: start from N months ago
        today = datetime.date.today()
        start_month = datetime.date(today.year, today.month, 1) - datetime.timedelta(days=30 * (months - 1))
        start_month = datetime.date(start_month.year, start_month.month, 1)
    
    # Calculate progressive order ranges for each month
    min_orders_base, max_orders_base = orders_per_month_range
    range_width = max_orders_base - min_orders_base  # Keep range width constant
    
    monthly_ranges = []
    current_min = min_orders_base
    
    for month_idx in range(months):
        if month_idx == 0:
            # First month uses base range
            monthly_ranges.append((int(current_min), int(current_min + range_width)))
        else:
            # Subsequent months grow by random 2-4%
            growth_rate = random.uniform(growth_rate_range[0], growth_rate_range[1])
            current_min = current_min * (1 + growth_rate)
            monthly_ranges.append((int(current_min), int(current_min + range_width)))
    
    # Print summary
    total_estimated_orders = sum(random.randint(r[0], r[1]) for r in monthly_ranges)
    
    print(f"\n{'='*70}")
    print(f"Order Data Generation - Multi-threaded (Monthly with Growth)")
    print(f"{'='*70}")
    print(f"Output directory: {output_path}")
    print(f"Months: {months}")
    print(f"First month range: {monthly_ranges[0][0]:,}-{monthly_ranges[0][1]:,} orders")
    print(f"Last month range: {monthly_ranges[-1][0]:,}-{monthly_ranges[-1][1]:,} orders")
    print(f"Growth rate: {growth_rate_range[0]*100:.0f}%-{growth_rate_range[1]*100:.0f}% per month")
    print(f"Estimated total: ~{total_estimated_orders:,} orders")
    print(f"Threads: {max_workers}")
    print(f"Start month: {start_month.strftime('%Y-%m')}")
    print(f"Customers: {len(customer_ids) if customer_ids else 'synthetic'}")
    print(f"Products: {len(products) if products else 'synthetic'}")
    print(f"Write to MySQL: {'Yes' if write_to_db else 'No (CSV only)'}")
    print(f"{'='*70}\n")
    
    # Print CSV headers
    print("📋 CSV Schema (10 files per month):")
    print("   orders_{YYYYMMDD}_{1-10}.csv: order_id,customer_id,order_date,total_amount,payment_method_id,created_at,updated_at")
    print("   order_items_{YYYYMMDD}_{1-10}.csv: order_item_id,order_id,product_id,quantity,price,discount\n")
    
    # Initialize generator with starting IDs
    generator = OrderGenerator(
        customer_ids, 
        products, 
        payment_methods,
        start_order_id=start_order_id,
        start_order_item_id=start_order_item_id
    )
    
    # Prepare tasks - one per month with progressive growth
    tasks = []
    current_month = start_month
    for month_idx in range(months):
        min_orders, max_orders = monthly_ranges[month_idx]
        num_orders = random.randint(min_orders, max_orders)
        tasks.append((generator, current_month, num_orders, output_path, True, write_to_db, db_connection))
        
        # Move to next month
        if current_month.month == 12:
            current_month = datetime.date(current_month.year + 1, 1, 1)
        else:
            current_month = datetime.date(current_month.year, current_month.month + 1, 1)
    
    # Execute with thread pool
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_month = {
            executor.submit(generate_month_data, *task): task[1] 
            for task in tasks
        }
        
        # Progress bar
        if TQDM_AVAILABLE:
            pbar = tqdm(total=months, desc="Generating months", unit="month")
        
        # Collect results as they complete
        for future in as_completed(future_to_month):
            month_date = future_to_month[future]
            try:
                result = future.result()
                results.append(result)
                
                if TQDM_AVAILABLE:
                    pbar.update(1)
                    pbar.set_postfix({
                        'month': result['month_str'],
                        'file': result['file_date_str'],
                        'orders': f"{result['num_orders']:,}",
                        'items': f"{result['num_items']:,}"
                    })
                else:
                    print(f"✅ {result['month_str']} → {result['file_date_str']}: {result['num_orders']:,} orders, {result['num_items']:,} items")
                    
            except Exception as e:
                print(f"❌ Error generating {month_date.strftime('%Y-%m')}: {e}")
        
        if TQDM_AVAILABLE:
            pbar.close()
    
    # Sort results by month
    results.sort(key=lambda x: x['month'])
    
    # Summary
    total_orders = sum(r['num_orders'] for r in results)
    total_items = sum(r['num_items'] for r in results)
    
    print(f"\n{'='*70}")
    print(f"✅ Generation Complete!")
    print(f"{'='*70}")
    print(f"Total orders: {total_orders:,}")
    print(f"Total items: {total_items:,}")
    print(f"Files created: {len(results) * 2}")
    print(f"\nGenerated files:")
    for r in results:
        print(f"  📅 {r['month_str']} → {r['file_date_str']}: {r['num_orders']:,} orders")
        print(f"     - {r['orders_file'].name}")
        print(f"     - {r['items_file'].name}")
    print(f"{'='*70}\n")
    
    return results

# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Order Data Generator (Multi-threaded, Monthly)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 6 months with 1000-5000 orders per month
  python order_generator.py --months 6 --orders_per_month 1000-5000
  
  # Generate 12 months with 3000-8000 orders, 8 threads
  python order_generator.py --months 12 --orders_per_month 3000-8000 --threads 8
  
  # Custom output directory
  python order_generator.py --months 6 --orders_per_month 2000-6000 --out_dir ./data/orders
  
  # Specify start month (will generate data for that month)
  python order_generator.py --months 3 --orders_per_month 1000-5000 --start_month 2025-08
  
Note: File naming convention - order_20251201.csv contains data for November 2025
        """
    )
    
    parser.add_argument(
        "--out_dir",
        default="./order_data",
        help="Output directory for CSV files (default: ./order_data)"
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of months to generate (default: 6)"
    )
    parser.add_argument(
        "--orders_per_month",
        type=str,
        default="1000-5000",
        help="Range of orders per month, format: 'min-max' (default: 1000-5000)"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of concurrent threads (default: 4)"
    )
    parser.add_argument(
        "--start_month",
        type=str,
        default=None,
        help="Start month in YYYY-MM format (default: 6 months ago from today)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--write_to_mysql",
        action="store_true",
        help="Write data to MySQL database (single-threaded mode)"
    )
    parser.add_argument(
        "--start_order_id",
        type=int,
        default=1,
        help="Starting order_id (default: 1)"
    )
    parser.add_argument(
        "--start_order_item_id",
        type=int,
        default=1,
        help="Starting order_item_id (default: 1)"
    )
    
    args = parser.parse_args()
    
    # Set random seed
    if args.seed:
        random.seed(args.seed)
        print(f"🎲 Random seed: {args.seed}")
    
    # Parse orders_per_month range
    if '-' in args.orders_per_month:
        parts = args.orders_per_month.split('-')
        orders_range = (int(parts[0]), int(parts[1]))
    else:
        count = int(args.orders_per_month)
        orders_range = (count, count)
    
    # Parse start_month
    start_month = None
    if args.start_month:
        try:
            year, month = map(int, args.start_month.split('-'))
            start_month = datetime.date(year, month, 1)
        except ValueError:
            print(f"⚠️  Invalid month format: {args.start_month}, using default")
    
    # Load data from database
    connection = create_db_connection()
    
    if connection:
        customer_ids = load_customer_ids(connection)
        products = load_products(connection)
        payment_methods = load_payment_methods(connection)
        
        # Keep connection open if writing to DB
        if not args.write_to_mysql:
            connection.close()
            connection = None
    else:
        print("⚠️  Using synthetic data (database not available)")
        customer_ids = None
        products = None
        payment_methods = None
    
    # Check MySQL write mode requirements
    if args.write_to_mysql and not connection:
        print("❌ Cannot write to MySQL: database connection failed")
        return
    
    # Generate orders
    start_time = datetime.datetime.now()
    
    generate_orders_multi_threaded(
        output_dir=args.out_dir,
        months=args.months,
        orders_per_month_range=orders_range,
        max_workers=args.threads,
        customer_ids=customer_ids,
        products=products,
        payment_methods=payment_methods,
        start_month=start_month,
        write_to_db=args.write_to_mysql,
        db_connection=connection,
        start_order_id=args.start_order_id,
        start_order_item_id=args.start_order_item_id
    )
    
    # Close connection if still open
    if connection:
        connection.close()
        print("✅ Database connection closed")
    
    elapsed = (datetime.datetime.now() - start_time).total_seconds()
    print(f"⏱️  Total time: {elapsed:.2f}s")

if __name__ == "__main__":
    main()
