from connect import create_connection, database


def get_tasks_by_user(conn, user_id):
    sql = '''
    SELECT * FROM tasks WHERE user_id = ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, (user_id,))
    return cur.fetchall()


def get_tasks_by_status(conn, status_name):
    sql = '''
    SELECT * FROM tasks WHERE status_id = (SELECT id FROM status WHERE name = ?);
    '''
    cur = conn.cursor()
    cur.execute(sql, (status_name,))
    return cur.fetchall()


def update_task_status(conn, task_id, new_status_name):
    sql = '''
    UPDATE tasks SET status_id = (SELECT id FROM status WHERE name = ?) WHERE id = ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, (new_status_name, task_id))
    conn.commit()


def get_users_without_tasks(conn):
    sql = '''
    SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM tasks);
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def add_task_for_user(conn, title, description, status_name, user_id):
    sql = '''
    INSERT INTO tasks (title, description, status_id, user_id) 
    VALUES (?, ?, (SELECT id FROM status WHERE name = ?), ?);
    '''
    cur = conn.cursor()
    cur.execute(sql, (title, description, status_name, user_id))
    conn.commit()


def get_uncompleted_tasks(conn):
    sql = '''
    SELECT * FROM tasks WHERE status_id != (SELECT id FROM status WHERE name = 'completed');
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def delete_task(conn, task_id):
    sql = '''
    DELETE FROM tasks WHERE id = ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, (task_id,))
    conn.commit()


def find_users_by_email(conn, email_pattern):
    sql = '''
    SELECT * FROM users WHERE email LIKE ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, (email_pattern,))
    return cur.fetchall()


def update_user_name(conn, user_id, new_name):
    sql = '''
    UPDATE users SET fullname = ? WHERE id = ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, (new_name, user_id))
    conn.commit()


def count_tasks_by_status(conn):
    sql = '''
    SELECT s.name, COUNT(t.id) 
    FROM tasks t
    JOIN status s ON t.status_id = s.id
    GROUP BY s.name;
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def get_tasks_by_user_email_domain(conn, domain):
    sql = '''
    SELECT t.* 
    FROM tasks t
    JOIN users u ON t.user_id = u.id
    WHERE u.email LIKE ?;
    '''
    cur = conn.cursor()
    cur.execute(sql, ('%' + domain,))
    return cur.fetchall()


def get_tasks_without_description(conn):
    sql = '''
    SELECT * FROM tasks WHERE description IS NULL OR description = '';
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def get_users_and_tasks_in_progress(conn):
    sql = '''
    SELECT u.fullname, t.title 
    FROM tasks t
    JOIN users u ON t.user_id = u.id
    JOIN status s ON t.status_id = s.id
    WHERE s.name = 'in progress';
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def get_users_and_task_count(conn):
    sql = '''
    SELECT u.fullname, COUNT(t.id) as task_count 
    FROM users u
    LEFT JOIN tasks t ON u.id = t.user_id
    GROUP BY u.fullname;
    '''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


if __name__ == '__main__':
    with create_connection(database) as conn:
        if conn is not None:
          
            tasks = get_tasks_by_user(conn, 1)
            print("Tasks for user_id=1:")
            for task in tasks:
                print(task)
            
            tasks = get_tasks_by_status(conn, 'new')
            print("Tasks with status 'new':")
            for task in tasks:
                print(task)

            update_task_status(conn, 1, 'in progress')
            print("Updated task status for task_id=1 to 'in progress'.")

            users = get_users_without_tasks(conn)
            print("Users without tasks:")
            for user in users:
                print(user)

            add_task_for_user(conn, 'New Task', 'This is a new task', 'new', 1)
            print("Added new task for user_id=1.")

            tasks = get_uncompleted_tasks(conn)
            print("Uncompleted tasks:")
            for task in tasks:
                print(task)

            delete_task(conn, 1)
            print("Deleted task with task_id=1.")

            users = find_users_by_email(conn, '%@example.com')
            print("Users with email pattern %@example.com:")
            for user in users:
                print(user)

            update_user_name(conn, 1, 'New Name')
            print("Updated name for user_id=1.")

            status_count = count_tasks_by_status(conn)
            print("Task count by status:")
            for status in status_count:
                print(status)

            tasks = get_tasks_by_user_email_domain(conn, 'example.com')
            print("Tasks for users with email domain example.com:")
            for task in tasks:
                print(task)

            tasks = get_tasks_without_description(conn)
            print("Tasks without description:")
            for task in tasks:
                print(task)

            users_tasks = get_users_and_tasks_in_progress(conn)
            print("Users and tasks in progress:")
            for user_task in users_tasks:
                print(user_task)

            user_task_count = get_users_and_task_count(conn)
            print("Users and their task count:")
            for user in user_task_count:
                print(user)
        else:
            print("Error! cannot create the database connection.")
