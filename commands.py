# Learning PostgreSQL

import psycopg2
from config import config

def execute_single(command):
    """
    Connect to the PostgreSQL database server and execute a command by passing on the command param
    """
    conn = None
    print("Estabilishing connection to the PostgreSQL database, hold on...")
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.execute(command)
        
        cur.close()
        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Fatal Error:", error)
    finally:
        if conn is not None:
            conn.close()

def insert_vendor(vendor_name):
    """insert a new vendor in the vendors table"""
    
    sql = """INSERT INTO vendors(vendor_name)
    VALUES(%s) RETURNING vendor_id;
    """
    
    conn = None
    vendor_id = None
    print("Estabilishing connection to the PostgreSQL database, hold on...")
    
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.execute(sql, (vendor_name,))
        
        vendor_id = cur.fetchone()[0]
        
        conn.commit()
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Fatal", error)
    finally:
        if conn is not None:
            conn.close()
            
    return vendor_id

def insert_vendors_list(vendor_list):
    """insert multiple vendors in the vendors table"""
    
    sql = "INSERT INTO vendors(vendor_name) VALUES(%s)"
    
    conn = None
    print("Estabilishing connection to the PostgreSQL database, hold on...")
    
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.executemany(sql, vendor_list)
        
        conn.commit()
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Fatal", error)
    finally:
        if conn is not None:
            conn.close()
            
def update_value(table, name_column, id_column, newName, id):
    """update a name based on its id"""
    sql = f"""UPDATE {table} 
    SET {name_column} = %s
    WHERE {id_column} = %s
    """
    
    conn = None
    updated_rows = 0
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.execute(sql, (newName, id))
        
        updated_rows = cur.rowcount
        
        conn.commit()
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Fatal", error)
    finally:
        if conn is not None:
            conn.close()
            
    return updated_rows

def add_part(insert_row, name_in_the_first_insert_row, name_to_insert, returning_value, assign_row, assign_param_one, assign_param_two, list):
    insert = f"INSERT INTO {insert_row}({name_in_the_first_insert_row}) VALUES(%s) RETURNING {returning_value};"
    assign = f"INSERT INTO {assign_row}({assign_param_one}, {assign_param_two}) VALUES(%s, %s);"
    
    conn = None
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.execute(insert, (name_to_insert,))
        
        part_id = cur.fetchone()[0]
        
        for vendor_id in list:
            cur.execute(assign, (vendor_id, part_id))
        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete(row, id_row, item_id):
    conn = None
    rows_deleted = 0
    
    try:
        params = config()
        
        conn = psycopg2.connect(**params)
        
        cur = conn.cursor()
        
        cur.execute(f"DELETE FROM {row} WHERE {id_row} = {item_id}")
        
        rows_deleted = cur.rowcount
        
        conn.commit()
        
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
    return rows_deleted

