from sqlite3 import Error
from connect import create_connection, database
from faker import Faker


fake = Faker()

def create_user(conn, user):
    sql = '''
    INSERT INTO users(fullname, email) VALUES(?,?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, user)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
        
    return cur.lastrowid


def create_status(conn, status):
    sql = '''
    INSERT INTO status(name) VALUES(?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, status)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
        
    return cur.lastrowid


def create_task(conn, task):
    sql = '''
    INSERT INTO tasks(title, description, status_id, user_id) VALUES(?,?,?,?);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, task)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
        
    return cur.lastrowid


if __name__ == '__main__':
    with create_connection(database) as conn:
        if conn is not None:
            for _ in range(5):
                user = (fake.name(), fake.email())
                create_user(conn, user)

            statuses = [('new',), ('in progress',), ('completed',)]
            for status in statuses:
                create_status(conn, status)

            for _ in range(10):
                task = (
                    fake.sentence(nb_words=5), 
                    fake.text(),  
                    fake.random_int(min=1, max=3), 
                    fake.random_int(min=1, max=5) 
                )
                create_task(conn, task)
        else:
            print("Error! cannot create the database connection.")
