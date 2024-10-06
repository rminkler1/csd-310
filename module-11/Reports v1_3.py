# CSD 310
# October 6, 2024
# Milestone 3

# Group 3 Reports
# Report 1: Ian
# Employee time history showing each employee and how many hours they worked each quarter for the last four quarters.
#
# Report 2: Robert
# What wines and how much of each wine has each distributor purchased over the last year?
#
# Report 3: Mikaela
# Suppliers delivery report: show each month for each supplier.
# Are all deliveries on time this month? If not, how many days late are their deliveries on average for the month?
#
# Report 4: Hector
# How much of each wine has sold over the last year.

from sqlite3 import Cursor
import mysql.connector
import pandas as pd

def report1_employee_hours(cursor):
    """
    # ****************************
    # Report # 1 Employee Data ***
    # ****************************
    """
    pass

def report2_wine_sales_by_distributor(cursor):
    """
    # *************************
    # Report # 2 Sales Data ***
    # *************************
    """

    # Create a view if it doesn't exist to simplify this report.
    cursor.execute("""
    CREATE OR REPLACE VIEW order_detail AS
    SELECT  so.order_date,
            dis.distributor_name,
            items.transaction_num,
            items.order_item,
            prod.product_name,
            items.quantity
    FROM sales_orders AS so
    JOIN distributors AS dis ON so.distributor_id = dis.distributor_id
    JOIN items_sold AS items ON so.transaction_num = items.transaction_num
    JOIN products AS prod ON prod.product_id = items.product_id;
    """)

    # What wines and how much of each wine has each distributor purchased over the last year?
    cursor.execute("""
    SELECT distributor_name, product_name, sum(quantity) AS qty
    FROM order_detail
    WHERE order_date > DATE_SUB(now(), INTERVAL 1 YEAR)
    GROUP BY distributor_name, product_name
    ORDER BY distributor_name, qty DESC;
    """)

    # fetch results from the database
    results = cursor.fetchall()

    # Set variables for string formatting
    dist = "Distributor"
    wine = "Wine"
    qty = "Quantity"

    # Display Report title
    print(f"\n\t\t\t****************************")
    print(  f"\t\t\t*** Sales by distributor ***")
    print(  f"\t\t\t***    Last 12 Months    ***")
    print(  f"\t\t\t****************************")

    print(f"{dist:25s}{wine:15s}{qty:>13s}")

    # Tracks previous distributor printed. Starts at None
    prev_distributor = None

    # Display Report data
    for result in results:

        # If this distributor is different from the previous distributor, insert a blank line
        if prev_distributor != result[0]:
            print()

        # set previous distributor to current distributor
        prev_distributor = result[0]

        # Print current data
        print(f"{result[0]:25s}{result[1]:15s}{result[2]:7n} Cases")

def report3_supplier_delivery(cursor):
    """
    # ********************************
    # Report # 3 Supplier Delivery ***
    # ********************************
    """

    # Define the year and months for the report
    year = 2024
    months = ['04', '05', '06']  # April, May, June

    # SQL query to fetch delivery data for April to June 2024
    sql = f"""
    SELECT 
        sup.supplier_name,
        MONTH(st.delivery_date) AS delivery_month,
        COUNT(st.delivery_date) AS total_deliveries,
        SUM(CASE WHEN st.delivery_date <= st.est_delivery_date THEN 1 ELSE 0 END) AS on_time_deliveries,
        AVG(CASE WHEN st.delivery_date > st.est_delivery_date THEN DATEDIFF(st.delivery_date, st.est_delivery_date) ELSE NULL END) AS average_days_late
    FROM 
        shipment_tracking st
    JOIN supply_orders so ON st.shipment_id = so.shipment_id
    JOIN suppliers sup ON sup.supplier_id = so.supplier_id
    WHERE 
        YEAR(st.delivery_date) = %s AND MONTH(st.delivery_date) IN ({','.join(['%s'] * len(months))})
    GROUP BY 
        sup.supplier_name, delivery_month
    ORDER BY 
        delivery_month, sup.supplier_name
    """

    cursor.execute(sql, (year, months[0], months[1], months[2]))
    result = cursor.fetchall()

    # Create a DataFrame to display the results
    df = pd.DataFrame(result, columns=[
        'Supplier Name', 'Delivery Month', 'Total Deliveries',
        'On-Time Deliveries', 'Average Days Late'
    ])

    # Calculate for on-time deliveries
    df['All On Time'] = df['Total Deliveries'] == df['On-Time Deliveries']

    # Display Report title
    print(f"\n\t\t\t********************************")
    print(f"\t\t\t*** Supplier Delivery Report ***")
    print(f"\t\t\t***    April to June 202s    ***")
    print(f"\t\t\t********************************")

    # Print Report
    print(df)



def report4_wine_sales(cursor):
    """
    # *******************************
    # Report # 4 Total Wine Sales ***
    # *******************************
    """
    pass


# Establish a connection to MySQL
# We need to specify the host (usually localhost for local development),
# user (username for MySQL), password (your MySQL password).
try:

    # Database Login configuration
    config = {
        "user": "winery_user",
        "password": "Bacchus_SecretPassword#1",
        "host": "localhost",
        "database": "bacchus_winery",
        "raise_on_warnings": True
    }

    # connect to database
    db_connection = mysql.connector.connect(**config)

    # Create a cursor object to execute SQL queries
    cursor = db_connection.cursor()

    print("Connection to the Bacchus Winery database was successful!")

except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit(1)

try:
    # Run Report # 1
    report1_employee_hours(cursor)

    # Run Report # 2
    report2_wine_sales_by_distributor(cursor)

    # Run Report # 3
    report3_supplier_delivery(cursor)

    # Run Report # 4
    report4_wine_sales(cursor)

except mysql.connector.Error as err:
    print(f"Error: {err}")
    exit(1)

finally:
    # Close the connection
    cursor.close()
    db_connection.close()

    print("\nReports Complete. Database connection closed")