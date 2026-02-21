# simulate.py
# Continuously simulates real business events in Oracle.
# Debezium will pick up every INSERT / UPDATE / DELETE as a CDC event.
# Run this after seed.py has finished the initial data load.

import random
import time
import signal
from datetime import datetime
from faker import Faker
from db import get_db_connection

fake = Faker("hu_HU")

# ---------------------------------------------------------------
# Simulation config
# ---------------------------------------------------------------
SLEEP_SECONDS    = 1.5  # pause between event batches
EVENTS_PER_BATCH = 3    # how many DB operations per cycle
NUM_CUSTOMERS    = 100  # must match seed.py
NUM_PRODUCTS     = 50   # must match seed.py
NUM_CATEGORIES   = 10   # must match seed.py

# Probability weights for which event type fires each cycle
EVENT_WEIGHTS = {
    "new_customer":         0.05,  # 5%  — new customer registers
    "new_order":            0.45,  # 45% — most common: new order placed
    "update_order_status":  0.30,  # 30% — order moves through lifecycle
    "cancel_order":         0.05,  # 5%  — order gets cancelled
    "update_customer":      0.05,  # 5%  — customer updates their profile
    "restock_product":      0.05,  # 5%  — warehouse restocks a product
    "new_product":          0.05,  # 5%  — new product added to catalog
}

# Order status lifecycle: each status can only move forward
STATUS_TRANSITIONS = {
    "PENDING": "PROCESSING",
    "PROCESSING": "SHIPPED",
    "SHIPPED": "DELIVERED",
}

# ---------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------

def new_customer(cursor):
    """INSERT: a new customer signs up."""
    cursor.execute("""
        INSERT INTO app.customers
            (customer_id, first_name, last_name, email, mobile, city, country)
        VALUES
            (app.customer_seq.NEXTVAL, :1, :2, :3, :4, :5, :6)
    """, (
        fake.first_name(),
        fake.last_name(),
        f"{fake.user_name()}_{random.randint(10000,99999)}@{fake.free_email_domain()}",
        fake.phone_number(),
        fake.city(),
        "Hungary"
    ))
    return "INSERT customer"


def new_order(cursor):
    """INSERT: a customer places a new order."""
    cursor.execute("""
        INSERT INTO app.orders
            (order_id, customer_id, product_item_id, ordered_quantity,
             order_status, city, country, street)
        VALUES
            (app.order_seq.NEXTVAL, :1, :2, :3, 'PENDING', :4, :5, :6)
    """, (
        random.randint(1,NUM_CUSTOMERS),
        random.randint(1,NUM_PRODUCTS),
        random.randint(1,5),
        fake.city(),
        "Hungary",
        fake.street_address()
    ))
    return "INSERT order"


def new_product(cursor):
    """
    INSERT: a new product is added to the catalog.
    Also assigns it to 1-2 random categories via the junction table.
    products_x_category has no created_by — it is system-managed only.
    """
    adjectives = ["Premium", "Basic", "Pro", "Ultra", "Eco", "Smart", "Mini", "Deluxe"]
    nouns = ["Widget", "Gadget", "Device", "Tool", "Kit", "Pack", "Set", "Bundle"]

    name = f"{random.choice(adjectives)} {random.choice(nouns)} {random.randint(100, 999)}"
    price = round(random.uniform(500,50000),2)
    qty = random.randint(10,200)

    # Insert product — created_by set by BEFORE INSERT trigger (USER = 'APP')
    cursor.execute("""
        INSERT INTO app.products (item_id, product_name, quantity, unit_price)
        VALUES (app.product_seq.NEXTVAL, :1, :2, :3)
    """, (name,qty,price))

    # Get the newly generated item_id
    cursor.execute("SELECT app.product_seq.CURRVAL FROM dual")
    new_item_id = cursor.fetchone()[0]

    # Assign to 1-2 random categories
    # No created_by column — products_x_category is system-managed
    assigned = random.sample(range(1, NUM_CATEGORIES + 1), k=random.randint(1, 2))
    for category_id in assigned:
        cursor.execute("""
            INSERT INTO app.products_x_category (item_id, category_id)
            VALUES (:1, :2)
        """, (new_item_id, category_id))

    return f"INSERT product '{name}'"


def update_order_status(cursor):
    """
    UPDATE: moves a batch of orders one step forward in the lifecycle.
    PENDING -> PROCESSING -> SHIPPED -> DELIVERED
    Debezium emits a CDC event with before/after payload for each updated row.
    The BEFORE UPDATE trigger auto-sets modified_at and modified_by.
    """
    updated_total = 0
    for old_status, new_status in STATUS_TRANSITIONS.items():
        cursor.execute("""
            UPDATE app.orders
            SET order_status = :1
            WHERE order_status = :2
              AND ROWNUM <= 3
        """, (new_status, old_status))
        updated_total += cursor.rowcount
    return f"UPDATE {updated_total} order status(es)"


def cancel_order(cursor):
    """UPDATE: randomly cancels a PENDING or PROCESSING order."""
    cursor.execute("""
        UPDATE app.orders
        SET order_status = 'CANCELLED'
        WHERE order_status IN ('PENDING', 'PROCESSING')
          AND ROWNUM <= 1
    """)
    return f"CANCEL {cursor.rowcount} order(s)"


def update_customer(cursor):
    """UPDATE: customer moves to a new city — trigger sets modified_at/modified_by."""
    cursor.execute("""
        UPDATE app.customers
        SET city = :1
        WHERE customer_id = :2
    """, (fake.city(), random.randint(1, NUM_CUSTOMERS)))
    return "UPDATE customer city"


def restock_product(cursor):
    """UPDATE: warehouse adds stock to a product — trigger sets modified_at/modified_by."""
    added_stock = random.randint(10, 100)
    cursor.execute("""
        UPDATE app.products
        SET quantity = quantity + :1
        WHERE item_id  = :2
    """, (added_stock, random.randint(1, NUM_PRODUCTS)))
    return f"UPDATE product stock +{added_stock}"


# ---------------------------------------------------------------
# Event dispatcher
# ---------------------------------------------------------------

EVENT_HANDLERS = {
    "new_customer": new_customer,
    "new_order": new_order,
    "new_product": new_product,
    "update_order_status": update_order_status,
    "cancel_order": cancel_order,
    "update_customer": update_customer,
    "restock_product": restock_product,
}


def pick_events(n: int) -> list:
    """Pick n event types based on configured probability weights."""
    events = list(EVENT_WEIGHTS.keys())
    weights = list(EVENT_WEIGHTS.values())
    return random.choices(events, weights=weights, k=n)


# ---------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------

running = True

def handle_shutdown(sig, frame):
    global running
    print("\nShutdown signal received. Finishing current batch...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ---------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------

def run():
    print("=" * 55)
    print("  CDC Event Simulator")
    print(f"  Batch size : {EVENTS_PER_BATCH} events")
    print(f"  Interval   : {SLEEP_SECONDS}s")
    print("  Press Ctrl+C to stop gracefully")
    print("=" * 55)

    conn = get_db_connection()
    cycle = 0

    while running:
        cycle += 1
        results = []

        try:
            with conn.cursor() as cur:
                for event_type in pick_events(EVENTS_PER_BATCH):
                    handler = EVENT_HANDLERS[event_type]
                    result = handler(cur)
                    results.append(result)

            conn.commit()

            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] Cycle #{cycle:04d} → {' | '.join(results)}")

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Cycle #{cycle}: {e}")

        time.sleep(SLEEP_SECONDS)

    conn.close()
    print("Simulator stopped cleanly.")


if __name__ == "__main__":
    run()
