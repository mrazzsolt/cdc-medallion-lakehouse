# seed.py
# One-time initial data load into Oracle.
# Run this ONCE before starting the continuous simulation.
# Debezium will snapshot these records when the connector first starts.

import random
from faker import Faker
from db import get_db_connection

fake = Faker("hu_HU")
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------
# Configuration — how many records to generate
# ---------------------------------------------------------------
NUM_CATEGORIES = 10
NUM_PRODUCTS = 50
NUM_CUSTOMERS = 100
NUM_ORDERS = 300

    
def seed_categories(cursor):
    """
    Insert product categories.
    """
    categories = [
        "Electronics", "Clothing", "Food & Beverage",
        "Home & Garden", "Sports", "Toys", "Books",
        "Health & Beauty", "Automotive", "Music"
    ]
    for idx, name in enumerate(categories, start=1):
        cursor.execute("""
            INSERT INTO app.product_categories
                (category_id, category_name, created_by)
            VALUES
                (app.category_seq.NEXTVAL, :1, 'SEED')
        """, (name,))
    print(f"  Inserted {len(categories)} categories.")


def seed_products(cursor, num_products):
    """
    Insert products with a realistic name, price and stock quantity.
    """
    adjectives = ["Premium", "Basic", "Pro", "Ultra", "Eco", "Smart", "Mini", "Deluxe"]
    nouns = ["Widget", "Gadget", "Device", "Tool", "Kit", "Pack", "Set", "Bundle"]

    for i in range(num_products):
        name = f"{random.choice(adjectives)} {random.choice(nouns)} {i + 1}"
        price = round(random.uniform(500, 50000), 2)
        qty = random.randint(10, 500)
        cursor.execute("""
            INSERT INTO app.products
                (item_id, product_name, quantity, unit_price, created_by)
            VALUES
                (app.product_seq.NEXTVAL, :1, :2, :3, 'SEED')
        """, (name,qty,price))

    print(f"  Inserted {num_products} products.")


def seed_products_x_category(cursor, num_products, num_categories):
    """
    Assign each product to 1-3 random categories.
    """
    count = 0
    for item_id in range(1, num_products + 1):
        # Pick 1 to 3 unique categories for this product
        assigned = random.sample(range(1, num_categories + 1), k=random.randint(1, 3))
        for category_id in assigned:
            cursor.execute("""
                INSERT INTO app.products_x_category
                    (item_id, category_id)
                VALUES
                    (:1, :2)
            """, (item_id, category_id))
            count += 1

    print(f"  Inserted {count} product-category assignments.")


def seed_customers(cursor, num_customers):
    """
    Insert fake customers using the Hungarian locale (hu_HU).
    """
    for _ in range(num_customers):
        cursor.execute("""
            INSERT INTO app.customers
                (customer_id, first_name, last_name, email,
                    mobile, city, country, created_by)
            VALUES
                (app.customer_seq.NEXTVAL, :1, :2, :3, :4, :5, :6, 'SEED')
        """, (
            fake.first_name(),
            fake.last_name(),
            # Make email unique: add random suffix to avoid UNIQUE constraint violations
            f"{fake.user_name()}_{random.randint(1000,9999)}@{fake.free_email_domain()}",
            fake.phone_number(),
            fake.city(),
            "Hungary"
        ))
    print(f"  Inserted {num_customers} customers.")


def seed_orders(cursor, num_customers, num_products, num_orders):
    """
    Insert orders with a realistic mix of statuses.
    """
    statuses = ["PENDING"]

    for _ in range(num_orders):
        cursor.execute("""
            INSERT INTO app.orders
                (order_id, customer_id, product_item_id, ordered_quantity,
                 order_status, city, country, street, created_by)
            VALUES
                (app.order_seq.NEXTVAL, :1, :2, :3, :4, :5, :6, :7, 'SEED')
        """, (
            random.randint(1,num_customers),
            random.randint(1,num_products),
            random.randint(1,10),
            statuses[0],
            fake.city(),
            "Hungary",
            fake.street_address()
        ))

    print(f"  Inserted {num_orders} orders.")


def run():
    print("Starting initial data seed...")
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            print("\n[1/5] Seeding product categories...")
            seed_categories(cur)

            print("[2/5] Seeding products...")
            seed_products(cur, NUM_PRODUCTS)

            print("[3/5] Seeding product-category assignments...")
            seed_products_x_category(cur, NUM_PRODUCTS, NUM_CATEGORIES)

            print("[4/5] Seeding customers...")
            seed_customers(cur, NUM_CUSTOMERS)

            print("[5/5] Seeding orders...")
            seed_orders(cur, NUM_CUSTOMERS, NUM_PRODUCTS, NUM_ORDERS)

        # Commit everything in one transaction
        conn.commit()
        print("\nSeed complete! Summary:")
        print(f"  {NUM_CATEGORIES} categories")
        print(f"  {NUM_PRODUCTS} products")
        print(f"  {NUM_CUSTOMERS} customers")
        print(f"  {NUM_ORDERS} orders")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    run()
