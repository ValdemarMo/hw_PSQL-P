import psycopg2

def create_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email  VARCHAR(40) UNIQUE
        );
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            phone VARCHAR(20) UNIQUE,
            client_id INTEGER NOT NULL REFERENCES clients(id)
        );
        """)

def add_client(conn, first_name, last_name, email, phone=None):
    conn.execute("""
        INSERT INTO clients(first_name, last_name, email) 
        VALUES (%s, %s, %s)
        RETURNING id;
        """, (first_name, last_name, email))
    client_id = conn.fetchone()
    if phone:
        conn.execute("""
                INSERT INTO phones(client_id, phone) 
                VALUES (%s, %s)
                RETURNING id;
                """, (client_id, phone))

def add_phone(conn, client_id, phone):
    conn.execute("""
        INSERT INTO phones(client_id, phone) 
        VALUES (%s, %s);
           """, (client_id, phone))

def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    if email:
        conn.execute("""
                UPDATE clients
                SET email = %s
                WHERE id = %s
                """, (email, client_id))
    if first_name:
        conn.execute("""
                UPDATE clients
                SET first_name = %s
                WHERE id = %s
                """, (first_name, client_id))
    if last_name:
        conn.execute("""
                UPDATE clients
                SET last_name = %s
                WHERE id = %s
                """, (last_name, client_id))

def delete_phone(conn, client_id=None, phone=None):
    conn.execute("""
            DELETE FROM phones 
            WHERE id = %s or phone = %s
               """, (client_id, phone))

def delete_client(conn, client_id):
    delete_phone(conn, client_id, None)
    conn.execute("""
                DELETE FROM clients 
                WHERE id = %s
                   """, (client_id))

def find_client(conn, **data):
    q = "SELECT first_name, last_name, email, p.phone FROM clients c \
            JOIN phones p ON c.id = p.client_id \
            WHERE " + ' and '.join(f"{x1} like '{x2}'" for x1, x2 in data.items())
    conn.execute(q)
    return conn.fetchall()

def all_client_phone(conn):
    q = "SELECT email, first_name, last_name, p.phone FROM clients c  \
        LEFT JOIN phones p ON c.id = p.client_id \
        ORDER BY email"
    conn.execute(q)
    return conn.fetchall()

with psycopg2.connect(database="c_db3", user="postgres", password="openBD") as conn:
    with conn.cursor() as c:
        create_db(c)
        add_client(c, 'x1', 'y1', 'x1@x.xxx', '+70001')
        add_client(c, 'x2', 'y2', 'x2@x.xxx')
        add_client(c, 'x3', 'y3', 'x3@x.xxx', '+70003')
        add_phone(c, '1', '+70008')
        add_phone(c, '1', '+70007')
        change_client(c, '1', None, 'y111', 'x11@x.xxx')
        delete_client(c, '8')
        delete_phone(c, '1', '+70007')
        xxx = all_client_phone(c)
        print(xxx)
        xxx = find_client(c, last_name='y111')
        print(xxx)
        conn.commit()
conn.close()


