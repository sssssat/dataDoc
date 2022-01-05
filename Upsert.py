import psycopg2 as psy
from psycopg2 import Error
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

SCHEMA = "public"
DOCKER_USERNAME = "data_team_read_user"
DOCKER_PASSWORD = "xc<k.$fw[D7djESk"
DOCKER_DB_NAME = "foodstory_prod"
DOCKER_HOST = "127.0.0.1"
DOCKER_PORT = "5467"
# ใช้ docker engine ของ psycopg2 เพราะว่า sqlalchemy ใช้ sub query ไม่ได้
DOCKER_ENGINE = psy.connect(host=DOCKER_HOST, database=DOCKER_DB_NAME, user=DOCKER_USERNAME, password=DOCKER_PASSWORD,port=DOCKER_PORT)
# DOCKER_ENGINE = create_engine("postgresql://" + DOCKER_USERNAME + ":" + quote_plus(DOCKER_PASSWORD) + "@" + DOCKER_HOST + ":" + DOCKER_PORT + "/" + DOCKER_DB_NAME)

sql = """SELECT uuid , ref_code , rest_name , branch_name , SKU , GMV , quantity , due , crt
    FROM (
    SELECT branch.internal_ref_code AS ref_code, rest."name" AS rest_name , branch."name" AS branch_name ,  pro_mas.product_name_th AS SKU ,
    CASE WHEN po_map.vat_flag = 1 THEN (po_map.product_price * po_map.product_quantity) * 1.07
    ELSE (po_map.product_price * po_map.product_quantity)
    END AS GMV , po_map.product_quantity AS quantity, mpo.due_date AS due , po.create_dt AS crt , po_map.id , branch.internal_ref_code || ' ' || pro_mas.mkp_product_master_code || ' ' || po.create_dt AS uuid
    FROM tbl_inventory_po AS po
    LEFT JOIN tbl_branch AS branch ON branch.branch_id = po.branch_id
    LEFT JOIN tbl_restaurant AS rest ON rest.restaurant_id = branch.restaurant_id
    LEFT JOIN mkp_inventory_sub_po AS mpo ON mpo.master_po_id = po.id
    LEFT JOIN mkp_product_inventory_po_map AS po_map ON po_map.mkp_sub_po_id = mpo.mkp_sub_po_id
    LEFT JOIN mkp_product AS pro ON pro.mkp_product_id = po_map.mkp_product_id
    JOIN mkp_product_master AS pro_mas ON pro_mas.mkp_product_master_code = pro.product_code
    WHERE to_char(po.create_dt,'YYYY-MM') LIKE '2022-01'
    AND po.market_place_flag = 1
    AND branch.verify_type != 2
    AND po.delete_flag = 0
    AND branch.delete_flag = 0
    AND po.po_status Not IN (2,3,5,100,101,102)
    AND mpo.po_status Not IN (2,3,5,100,101,102)
    AND branch.name not in ('%Test%','test','Demo Market - PO') and lower(branch.name) not like '%test%'
    and rest.name not like '%Tum Hai%') AS a"""
SKU_last_month = pd.read_sql(sql, DOCKER_ENGINE)

SCHEMA = "data_warehouse"
DOCKER_USERNAME = "internal_data_admin"
DOCKER_PASSWORD = "y4UrTWF#5Eq!TYGJ"
DOCKER_DB_NAME = "internal"
DOCKER_HOST = "127.0.0.1"
DOCKER_PORT = "5454"
DOCKER_ENGINE2 = psy.connect(host=DOCKER_HOST, database=DOCKER_DB_NAME, user=DOCKER_USERNAME,password=DOCKER_PASSWORD, port=DOCKER_PORT, options="-c search_path=" + SCHEMA)
# DOCKER_ENGINE = create_engine("postgresql://" + DOCKER_USERNAME + ":" + quote_plus(DOCKER_PASSWORD) + "@" + DOCKER_HOST + ":" + DOCKER_PORT + "/" + DOCKER_DB_NAME)

def write_table(data,TABLE_NAME):
    data.to_sql(name=TABLE_NAME, con=DOCKER_ENGINE, schema = SCHEMA, if_exists='replace', index=False)
    return None

def execute_mogrify(conn, datafrm, table):
    responder_id_not_unique = datafrm.drop('uuid', 1)
    upsert_value = ["EXCLUDED." + s for s in responder_id_not_unique.columns]

    tpls = [tuple(x) for x in datafrm.to_numpy()]
    cols = ','.join(list(datafrm.columns))
    cols_2 = ','.join(list(responder_id_not_unique.columns))
    cols_3 = ','.join(upsert_value)

    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)",tup).decode('utf8') for tup in tpls]
    sql = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values) + " ON CONFLICT (uuid) DO UPDATE SET (" + cols_2 + ")" + " = (" + cols_3 + ")"

    cursor.execute(sql)
    print("Data inserted using execute_mogrify() successfully.")
    conn.commit()
    cursor.close()

execute_mogrify(DOCKER_ENGINE2, SKU_last_month , "sku_cleansing_data")