import psycopg2
import requests

case = {
    'database': 'school',
    'user': 'postgres',
    'host': 'localhost',
    'password': '1234',
    'port': 5432
}

product_url = requests.get('https://dummyjson.com/products')
my_product_list = product_url.json()['products']


class ContextManager:
    def __init__(self):
        self.connect = psycopg2.connect(**case)
        self.cur = self.connect.cursor()

    def __enter__(self):
        return self.connect, self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur and not self.connect.closed:
            self.cur.close()
        if self.connect and not self.connect.closed:
            self.connect.close()

    @staticmethod
    def create_table_query():
        create_table_queries = '''CREATE TABLE products(
                                    id SERIAL PRIMARY KEY,
                                    title VARCHAR(250) NOT NULL,
                                    description TEXT NOT NULL,
                                    category VARCHAR(250) NOT NULL,
                                    price NUMERIC(10,2) NOT NULL,
                                    discount NUMERIC(10,2),
                                    rating REAL,
                                    stock INT,
                                    tags TEXT,
                                    sku VARCHAR(250) NOT NULL,
                                    weight INT
                        );'''
        return create_table_queries

    @staticmethod
    def insert_into_table():
        insert_query = '''INSERT INTO products(
                            title, 
                            description, 
                            category, 
                            price, 
                            discount, 
                            rating, 
                            stock, 
                            tags, 
                            sku, 
                            weight)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
        return insert_query


my_product = ContextManager()

with my_product as (conn, cur):
    while True:
        try:
            choice: str = int(input('1 => Create table\n2 => Insert data\n3 => exit: '))
            for choice in range(1, choice + 1):
                if choice == 1:
                    table = ContextManager.create_table_query()
                    cur.execute(table)
                    conn.commit()
                    print("Successfully created table")
                elif choice == 2:
                    query = ContextManager.insert_into_table()
                    for product in my_product_list:
                        tags = ', '.join(product['tags'])
                        cur.execute(query, (product['title'],
                                            product['description'],
                                            product['category'],
                                            product['price'],
                                            product['discount'],
                                            product['rating'],
                                            product['stock'],
                                            tags,
                                            product['sku'],
                                            product['weight']))
                        conn.commit()
                    print("Successfully inserted data")

                elif choice == 3:
                    print("Successfully exit")

        except ValueError as e:
            print(e)
