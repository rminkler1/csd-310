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

import mysql.connector
import pandas as pd

def report1_employee_hours(cursor):
    """
    # ****************************
    # Report # 1 Employee Data ***
    # ****************************
    """

    # SQL query to retrieve quarterly timekeeping records along with employee names
    sql = """
    SELECT e.first_name, e.last_name, t.punch_datetime, t.in_or_out
    FROM timekeeping t
    JOIN employees e ON t.employee_id = e.employee_id
    ORDER BY e.last_name, t.punch_datetime
    """
    
    cursor.execute(sql)
    timekeeping_records = cursor.fetchall()

    # Debug print statement to check fetched records
    print(f"Fetched Timekeeping Records: {timekeeping_records}")  # Debug statement

    return timekeeping_records  # Ensure this is present

# Function to determine the quarter from a date
def get_quarter(date):
    quarter = None
    if 1 <= date.month <= 3:
        quarter = "Q1"
    elif 4 <= date.month <= 6:
        quarter = "Q2"
    elif 7 <= date.month <= 9:
        quarter = "Q3"
    else:
        quarter = "Q4"
    
    print(f"Date: {date}, Quarter: {quarter}")  # Debug statement
    return quarter

# Calculates total hours worked per quarter for each employee
def calculate_hours_per_quarter(records):
    employee_data = {}
    current_employee = None
    last_punch_in = None

    for record in records:
        first_name, last_name, punch_datetime, in_or_out = record
        print(f"Processing record: {record}")  # Debug statement to show each record being processed
        employee_key = f"{first_name} {last_name}"

        if employee_key not in employee_data:
            employee_data[employee_key] = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "Total": 0}

        if in_or_out == "IN":
            last_punch_in = punch_datetime
        elif in_or_out == "OUT" and last_punch_in:
            # Calculates hours worked between IN and OUT punches
            time_diff = punch_datetime - last_punch_in
            hours_worked = time_diff.total_seconds() / 3600  # Convert to hours

            # Determines quarter for this punch-out time
            quarter = get_quarter(punch_datetime)

            # Adds hours to appropriate quarter
            employee_data[employee_key][quarter] += hours_worked
            employee_data[employee_key]["Total"] += hours_worked

            # Resets last_punch_in
            last_punch_in = None

    return employee_data

# Displays report showing hours per quarter and total hours for each employee
def display_quarterly_report(employee_data):
    for employee, hours in employee_data.items():
        print(f"\nEmployee: {employee}")
        print("=" * 30)
        for quarter in ["Q1", "Q2", "Q3", "Q4"]:
            print(f"{quarter}: {hours[quarter]:.2f} hours")
        print(f"Total hours worked across all quarters: {hours['Total']:.2f} hours")


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
        pass

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
    pass



def report4_wine_sales(cursor):
    """
    # *******************************
    # Report # 4 Total Wine Sales ***
    # *******************************
    """

    # SQL query to get total wine sales for the last year
    sql = """
    SELECT product_name, SUM(quantity) AS total_quantity
    FROM items_sold AS i
    JOIN products AS p ON i.product_id = p.product_id
    JOIN sales_orders AS s ON i.transaction_num = s.transaction_num
    WHERE s.order_date > DATE_SUB(NOW(), INTERVAL 1 YEAR)
    GROUP BY product_name
    ORDER BY total_quantity DESC;
    """

    # Execute the query to fetch wine sales data
    cursor.execute(sql)
    results = cursor.fetchall()

    # Set up report header
    print("\n\t\t*******************************")
    print("\t\t*** Total Wine Sales Report ***")
    print("\t\t***     Last 12 Months      ***")
    print("\t\t*******************************\n")

    # Format and print the column headers
    print(f"{'Wine':25s}{'Total Quantity':>15s}")

    # Iterate through the fetched results and print each row
    for result in results:
        product_name, total_quantity = result

        # Print the product name (wine) and total quantity sold
        print(f"{product_name:25s}{total_quantity:>15f}")

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
    # Run Report # 1 and capture the returned timekeeping records
    timekeeping_records = report1_employee_hours(cursor)

    # Check if timekeeping_records is not empty
    print(f"Timekeeping Records: {timekeeping_records}")  # Debug statement

    # Generate report showing hours per quarter for each employee
    employee_data = calculate_hours_per_quarter(timekeeping_records)

    # Display the final report
    display_quarterly_report(employee_data)

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
