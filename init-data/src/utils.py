import psycopg2

def get_conn(database: str="postgres",
             user: str="postgres",
             password: str="123",
             host="localhost",
             port=5432):
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        print("Connection to the PostgreSQL established successfully.")
            
        return conn
    except Exception as e:
        print(f"Connection to the PostgreSQL encountered and error {e}.")
        return False