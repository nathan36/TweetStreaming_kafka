import sqlalchemy as sa
from sqlalchemy.engine import Connection

def create_connection(user, pin, host, port, dbname):
    connection_str = 'mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(user, pin, host, port, dbname)
    engine = sa.create_engine(connection_str)
    return engine.connect()

def write_to_db(conn, data: list, table: str):
    for row in data:
        values = ','.join(['?'] * len(row.values()))
        query = 'insert into {}({}) values({})'.format(table, ','.join(row.keys()), values)
        conn.execute(query, [*row.values()])