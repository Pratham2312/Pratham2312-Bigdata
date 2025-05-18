# # import pandas as pd
# # import sqlite3

# # def process_data():

# #     # Loading the transactions data from the CSV file into a pandas DataFrame
# #     file_path = r"src/data/transactions.csv" 
# #     df = pd.read_csv(file_path, encoding="utf-8")
    
# #     # Removing any rows with missing values in the DataFrame (Use dropna or another method)
# #     df.dropna(inplace=True)  # You can change this to other methods if required

# #     # Converting the 'TransactionDate' column to a datetime format using pandas
# #     df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

# #     # Setting up a connection to SQLite database and create a table if it doesn't exist
# #     conn = sqlite3.connect("src/data/transactions.db")
# #     cursor = conn.cursor()

# #     cursor.execute("""
# #     CREATE TABLE IF NOT EXISTS transactions (
# #         transaction_id INTEGER PRIMARY KEY,
# #         customer_id INTEGER,
# #         product TEXT,
# #         amount REAL,
# #         TransactionDate TEXT,
# #         PaymentMethod TEXT,
# #         City TEXT,
# #         Category TEXT
# #     )
# #     """)
    
# #     # TO DO: Insert data into the database
# #     # Your task: Insert the cleaned DataFrame into the SQLite database. Ensure to replace the table if it already exists.
# #     df.to_sql("Write your query")

# #     # Example Queries - Write SQL queries based on the instructions below

# #     # TO DO: Query for Top 5 Most Sold Products
# #     # Your task: Write an SQL query to find the top 5 most sold products based on transaction count.
# #     cursor.execute("""  Enter your query  """)


# #     # TO DO:  Query for Monthly Revenue Trend
# #     # Your task: Write an SQL query to find the total revenue per month.
# #     cursor.execute("""  Enter your query  """)

# #     # TO DO:  Query for Payment Method Popularity
# #     # Your task: Write an SQL query to find the popularity of each payment method used in transactions.
# #     cursor.execute("""  Enter your query  """)


# #     # TO DO:  Query for Top 5 Cities with Most Transactions
# #     # Your task: Write an SQL query to find the top 5 cities with the most transactions.
# #     cursor.execute("""  Enter your query  """)


# #     # TO DO:  Query for Top 5 High-Spending Customers
# #     # Your task: Write an SQL query to find the top 5 customers who spent the most in total.
# #     cursor.execute("""  Enter your query  """)


# #     # TO DO:  Query for Hadoop vs Spark Related Product Sales
# #     # Your task: Write an SQL query to categorize products related to Hadoop and Spark and find their sales.
# #     cursor.execute("""  Enter your query  """)


# #     # TO DO:  Query for Top Spending Customers in Each City
# #     # Your task: Write an SQL query to find the top spending customer in each city using subqueries.
# #     cursor.execute("""  Enter your query  """)


# #     # Step 8: Close the connection
# #     # Your task: After all queries, make sure to commit any changes and close the connection
# #     conn.commit()
# #     conn.close()
# #     print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

# # if __name__ == "__main__":
# #     process_data()
# import pandas as pd
# import sqlite3

# def process_data():
#     # Step 1: Load CSV
#     file_path = r"src/data/transactions.csv"
#     df = pd.read_csv(file_path, encoding="utf-8")
#     df.dropna(inplace=True)
#     df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

#     # Step 2: Connect to SQLite and create table
#     conn = sqlite3.connect("src/data/transactions.db")
#     cursor = conn.cursor()

#     cursor.execute("DROP TABLE IF EXISTS transactions")

#     cursor.execute("""
#     CREATE TABLE transactions (
#         TransactionID INTEGER PRIMARY KEY,
#         CustomerID TEXT,
#         Product TEXT,
#         Amount REAL,
#         TransactionDate TEXT,
#         PaymentMethod TEXT,
#         City TEXT,
#         Category TEXT
#     )
#     """)

#     # Step 3: Insert data into DB
#     df.to_sql("transactions", conn, if_exists="replace", index=False)

#     # Step 4: Execute Queries

#     print("\n📊 Top 5 Best-Selling Products:")
#     for row in cursor.execute("""
#         SELECT Product, COUNT(*) AS SalesCount
#         FROM transactions
#         GROUP BY Product
#         ORDER BY SalesCount DESC
#         LIMIT 5
#     """):
#         print(row)

#     print("\n📈 Monthly Revenue Trend:")
#     for row in cursor.execute("""
#         SELECT strftime('%Y-%m', TransactionDate) AS Month, SUM(Amount) AS Revenue
#         FROM transactions
#         GROUP BY Month
#         ORDER BY Month
#     """):
#         print(row)

#     print("\n💳 Payment Method Popularity:")
#     for row in cursor.execute("""
#         SELECT PaymentMethod, COUNT(*) AS Count
#         FROM transactions
#         GROUP BY PaymentMethod
#         ORDER BY Count DESC
#     """):
#         print(row)

#     print("\n🏙️ Top 5 Cities with Most Transactions:")
#     for row in cursor.execute("""
#         SELECT City, COUNT(*) AS TransactionCount
#         FROM transactions
#         GROUP BY City
#         ORDER BY TransactionCount DESC
#         LIMIT 5
#     """):
#         print(row)

#     print("\n💰 Top 5 High-Spending Customers:")
#     for row in cursor.execute("""
#         SELECT CustomerID, SUM(Amount) AS TotalSpent
#         FROM transactions
#         GROUP BY CustomerID
#         ORDER BY TotalSpent DESC
#         LIMIT 5
#     """):
#         print(row)

#     print("\n🔥 Hadoop vs Spark Sales Comparison:")
#     for row in cursor.execute("""
#         SELECT 
#             CASE 
#                 WHEN Product LIKE '%Hadoop%' THEN 'Hadoop'
#                 WHEN Product LIKE '%Spark%' THEN 'Spark'
#                 ELSE 'Other'
#             END AS Category,
#             COUNT(*) AS SalesCount,
#             SUM(Amount) AS TotalRevenue
#         FROM transactions
#         GROUP BY Category
#     """):
#         print(row)

#     print("\n🏅 Top Spending Customers in Each City:")
#     for row in cursor.execute("""
#         SELECT City, CustomerID, TotalSpent FROM (
#             SELECT 
#                 City,
#                 CustomerID,
#                 SUM(Amount) AS TotalSpent,
#                 RANK() OVER (PARTITION BY City ORDER BY SUM(Amount) DESC) as rnk
#             FROM transactions
#             GROUP BY City, CustomerID
#         ) WHERE rnk = 1
#     """):
#         print(row)

#     # Step 5: Close DB
#     conn.commit()
#     conn.close()
#     print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

# # This makes sure your file runs when you execute it from the terminal
# if __name__ == "__main__":
#     process_data()
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

def process_data():
    # Load CSV data
    file_path = "src/data/transactions.csv"
    df = pd.read_csv(file_path, encoding="utf-8")

    # Clean data - drop rows with missing values
    df.dropna(inplace=True)

    # Convert TransactionDate to datetime
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Rename columns to match DB schema
    df.rename(columns={
        "TransactionID": "transaction_id",
        "CustomerID": "customer_id",
        "Product": "product",
        "Amount": "amount",
        "TransactionDate": "TransactionDate",  # Keep as-is for DB query
        "PaymentMethod": "PaymentMethod",
        "City": "City",
        "Category": "Category"
    }, inplace=True)

    # Connect to SQLite DB
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()

    # Create table (drop if exists)
    cursor.execute("DROP TABLE IF EXISTS transactions")
    cursor.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY,
            customer_id TEXT,
            product TEXT,
            amount REAL,
            TransactionDate TEXT,
            PaymentMethod TEXT,
            City TEXT,
            Category TEXT
        )
    """)

    # Insert data into DB
    df.to_sql("transactions", conn, if_exists="append", index=False)

    # 1. Top 5 Best-Selling Products
    query1 = """
    SELECT product, COUNT(*) AS sales_count
    FROM transactions
    GROUP BY product
    ORDER BY sales_count DESC
    LIMIT 5;
    """
    top_products = pd.read_sql_query(query1, conn)
    print("\nTop 5 Best-Selling Products:")
    print(top_products)

    plt.figure(figsize=(8,5))
    plt.bar(top_products['product'], top_products['sales_count'], color='skyblue')
    plt.title("Top 5 Best-Selling Products")
    plt.xlabel("Product")
    plt.ylabel("Number of Sales")
    plt.tight_layout()
    plt.show()
    plt.close()

    # 2. Monthly Revenue Trend
    query2 = """
    SELECT strftime('%Y-%m', TransactionDate) AS month, SUM(amount) AS total_revenue
    FROM transactions
    GROUP BY month
    ORDER BY month;
    """
    monthly_revenue = pd.read_sql_query(query2, conn)
    print("\nMonthly Revenue Trend:")
    print(monthly_revenue)

    plt.figure(figsize=(10,5))
    plt.plot(monthly_revenue['month'], monthly_revenue['total_revenue'], marker='o', color='green')
    plt.title("Monthly Revenue Trend")
    plt.xlabel("Month")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    plt.close()

    # 3. Payment Method Popularity
    query3 = """
    SELECT PaymentMethod, COUNT(*) AS transaction_count
    FROM transactions
    GROUP BY PaymentMethod
    ORDER BY transaction_count DESC;
    """
    payment_popularity = pd.read_sql_query(query3, conn)
    print("\nPayment Method Popularity:")
    print(payment_popularity)

    plt.figure(figsize=(7,7))
    plt.pie(payment_popularity['transaction_count'], labels=payment_popularity['PaymentMethod'], autopct='%1.1f%%', startangle=140)
    plt.title("Payment Method Popularity")
    plt.tight_layout()
    plt.show()
    plt.close()

    # 4. Top Cities with Most Transactions
    query_cities = """
    SELECT City, COUNT(*) AS txn_count
    FROM transactions
    GROUP BY City
    ORDER BY txn_count DESC
    LIMIT 5;
    """
    top_cities = pd.read_sql_query(query_cities, conn)
    print("\nTop Cities with Most Transactions:")
    print(top_cities)

    plt.figure(figsize=(8,5))
    plt.bar(top_cities['City'], top_cities['txn_count'], color='orange')
    plt.title("Top Cities with Most Transactions")
    plt.xlabel("City")
    plt.ylabel("Number of Transactions")
    plt.tight_layout()
    plt.show()
    plt.close()

    # 5. Top Spending Customers
    query_customers = """
    SELECT customer_id, SUM(amount) AS total_spent
    FROM transactions
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 5;
    """
    top_customers = pd.read_sql_query(query_customers, conn)
    print("\nTop 5 Spending Customers:")
    print(top_customers)

    plt.figure(figsize=(8,5))
    plt.bar(top_customers['customer_id'], top_customers['total_spent'], color='purple')
    plt.title("Top 5 Spending Customers")
    plt.xlabel("Customer ID")
    plt.ylabel("Total Spent")
    plt.tight_layout()
    plt.show()
    plt.close()

    # 6. Hadoop vs Spark Sales Comparison
    query_hadoop = """
    SELECT product, COUNT(*) AS sales_count
    FROM transactions
    WHERE product LIKE '%Hadoop%'
    GROUP BY product;
    """
    hadoop_sales = pd.read_sql_query(query_hadoop, conn)

    query_spark = """
    SELECT product, COUNT(*) AS sales_count
    FROM transactions
    WHERE product LIKE '%Spark%'
    GROUP BY product;
    """
    spark_sales = pd.read_sql_query(query_spark, conn)

    print("\nHadoop Product Sales:")
    print(hadoop_sales)

    print("\nSpark Product Sales:")
    print(spark_sales)

    # 7. Top Spending Customers by City (using window function)
    query_top_customer_city = """
    SELECT city, customer_id, total_spent FROM (
        SELECT City AS city, customer_id, SUM(amount) AS total_spent,
               ROW_NUMBER() OVER (PARTITION BY City ORDER BY SUM(amount) DESC) AS rank
        FROM transactions
        GROUP BY City, customer_id
    )
    WHERE rank = 1;
    """
    top_customer_city = pd.read_sql_query(query_top_customer_city, conn)
    print("\nTop Spending Customers by City:")
    print(top_customer_city)

    # Close connection
    conn.commit()
    conn.close()

    print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
