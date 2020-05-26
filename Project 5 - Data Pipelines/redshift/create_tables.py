import psycopg2
from config_details import get_cluster_details, get_endpoint
from create_redshift import user_feedback

KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = get_cluster_details()

user_feedback('Connecting to Redshift Cluster...')
conn_string = f"dbname={DWH_DB} port={DWH_PORT} user= {DWH_DB_USER} password= {DWH_DB_PASSWORD} host={get_endpoint()}"
#connect to Redshift (database should be open to the world)
conn = psycopg2.connect(conn_string)
user_feedback('Connected.')
user_feedback('Creating Tables...')
cur = conn.cursor()
cur.execute(open("../create_tables.sql", "r").read())
conn.close()
user_feedback('Tables Created.')
