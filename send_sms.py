#This script works with Ultimate SMS Platform to send SMS in massive way 
import mysql.connector as connection
import pandas as pd
import aiohttp
import asyncio
import time
import json
from concurrent.futures import ThreadPoolExecutor as th
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine

start_time = time.time()
pd.set_option('display.max_colwidth', None)

"""
#Conexion BD PORTAL
# Configuración de la conexión
db_config = {
    'user': 'root',
    'password': 'usms%D3m0.2022..',
    'host': '172.16.1.18',
    'database': 'usms',
    'port': 3306
}
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor(buffered=True, dictionary=True)
#engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config))
"""

async def perform_post_request(url, data):
    connector = aiohttp.TCPConnector(limit=10000)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as response:
            return await response.text()

async def send(x, cursor, user_id, campaign_id, sender_id, sending_server_id, content):
    print(x)
    proxy_url = "http://127.0.0.1:8080"
    data = {'src': sender_id, 'dst': x, 'text': content}
    status = await perform_post_request(proxy_url, data)
    print(status)
    response = 'Delivered'
    if status[0:3] != '200':
        response = 'Failed'
    time_update =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = "INSERT INTO `cg_reports` (`id`, `uid`, `user_id`, `campaign_id`, `from`, `to`, `message`, `media_url`, `sms_type`, `status`, `send_by`, `cost`, `api_key`, `sending_server_id`, `created_at`, `updated_at`, `operator`) VALUES (NULL, '666ccc777', %s, %s, '%s', '%s', '%s', NULL, 'plain', '%s', 'from', '1', NULL, %s, '%s', '%s', '')" % (user_id, campaign_id, sender_id, x, content, response, sending_server_id, time_update, time_update)
    print(sql)
    cursor.execute(sql)
    return status


async def main():

    threshold = 2000
    block_size = 200
    #threshold = 5
    #block_size = 4
    #num_threads = 8

    try:
        mydb = connection.connect(host="172.16.1.18", database = 'usms',user="root", passwd="usms%D3m0.2022..",use_pure=True)

        #Campañas con archivo de plantilla, que estan asociadas a la tabla cg_file_campaign_data
        sql_campaigns = """SELECT c.id AS id,
        c.campaign_name AS name,
        c.user_id AS user_id,
        s.sender_id AS sender,
        c.message AS content,
        c.schedule_time AS schedule_time,
        c.schedule_type AS schedule_type,
        c.cache AS cache,
        c.sending_server_id AS server_id
        FROM cg_campaigns c,
        cg_campaigns_senderids s,
        cg_sending_servers t
        WHERE c.id = s.campaign_id
        AND c.sending_server_id = t.id
        AND c.status IN ('queued', 'scheduled', 'processing')
        AND t.name = 'FastDB'"""

        #cursor.execute(sql_campaigns)
        #results = cursor.fetchall()
        cursor = mydb.cursor(buffered=True, dictionary=True)
        cursor.execute(sql_campaigns)

        for row in cursor:
            print(row)
            campaign_id = row['id']
            user_id = row['user_id']
            campaign_name = row['name']
            print(campaign_id)
            print(campaign_name)
            sender_id = row['sender']
            content = row['content']
            cache = row['cache']
            sending_server_id = row['server_id']
            dict_cache = json.loads(cache)
            cache_contact_count = dict_cache['ContactCount']
            cache_delivered_count = dict_cache['DeliveredCount']
            cache_failed_count = dict_cache['FailedDeliveredCount']
            cache_notdelivered_count = dict_cache['NotDeliveredCount']
            bExecute = False
            schedule_type = row['schedule_type']
            schedule_time = row['schedule_time']
            print(schedule_time)

            delta_seconds = 0
            if schedule_time is None:
                #ejecucion automatica, no programada
                bExecute = True
                delta_seconds = 64000
            else:
                delta_seconds = (datetime.now() - schedule_time).total_seconds()
                bExecute = True
            print("DELTA SECONDS: %s" % delta_seconds)

            if delta_seconds > 30:
                #UPDATE campo state in table cg_campaigns
                query = "UPDATE cg_campaigns SET status='processing' WHERE id=%s" % campaign_id
                cursor.execute(query)
                mydb.commit()
                print("Processing")

                sql_contact_count = "SELECT count(*) AS total FROM cg_file_campaign_data WHERE campaign_id = %s" % campaign_id
                cursor.execute(sql_contact_count)
                total_contacts = cursor.fetchone()['total']
                print("********************")
                print(total_contacts)
                print(type(total_contacts))

                sql_reports_delivered = "SELECT count(*) AS cantidad FROM cg_reports WHERE campaign_id = %s AND substring(status,1,9) = 'Delivered'" % campaign_id
                cursor.execute(sql_reports_delivered)
                total_delivered = cursor.fetchone()

                sql_reports_not_delivered = "SELECT count(*) AS cantidad FROM cg_reports WHERE campaign_id = %s AND substring(status,1,9) != 'Delivered'" % campaign_id
                cursor.execute(sql_reports_not_delivered)
                total_not_delivered = cursor.fetchone()

                sql_phones = "SELECT phone, message FROM  cg_file_campaign_data WHERE campaign_id = %s" % campaign_id
                print(sql_phones)
                df = pd.read_sql_query(sql_phones, mydb)
                print(df.head(20))
                total_campaign = len(df)
                print("Numero de Records a procesar: ")
                print(total_campaign)

                blocks = []
                if len(df) > threshold:
                    for i in range((len(df)//block_size)):
                        blocks.append(df[block_size*i:block_size*(i+1)])
                    i=i+1
                    if i*block_size < len(df):
                        blocks.append(df[block_size*i:])
                else:
                    blocks.append(df)
                #print(blocks)

                for i, block in enumerate(blocks):
                    #print("\n" +"="*40)
                    print("-"*40)
                    print("Block: " + str(i))
                    print(block)
                    block['status'] = await asyncio.gather(*(send(x, cursor, user_id, campaign_id, sender_id, sending_server_id, y) for x, y in zip(block['phone'], block['message'])), return_exceptions=True)
                    mydb.commit()
                    dict_cache['DeliveredCount'] += len(block)
                    cache = json.dumps(dict_cache)
                    print("-"*40)
                    print("Cache:")
                    print(cache)
                    new_cache = str(dict_cache).replace("'", "\"")
                    print("New Cache:")
                    print(new_cache)

                    #UPDATE campo "cache" in table cg_campaigns(
                    query_cache = """UPDATE cg_campaigns SET cache='%s' WHERE id=%s""" % (new_cache,campaign_id)
                    cursor.execute(query_cache)
                    mydb.commit()

                    print("\n" +"="*40)
                    print("Campaign ID: %s"%campaign_id)
                    print("Campaign Name: %s"%campaign_name)
                    print("--Cache: %s"%cache)
                    print("--Schedule Time: %s"%schedule_time)
                    print("--Schedule Type: %s"%schedule_type)
                    print("--Group: %d"%i)
                    print("--Message: %s"%content)
                    print("--Sender ID: %s"%sender_id)
                    #print("--Contact List ID: %s"%contact_list)
                    print(block.head(10))


                #CHECK IF CAMPAIGN COMPLETED 100%
                print("Validando si la campaña se completo al 100%\n")
                query_check = "SELECT cache FROM cg_campaigns WHERE id=%s" % campaign_id
                cursor.execute(query_check)
                cache_cycle = cursor.fetchone()['cache']
                print(cache_cycle)
                dict_cache_cycle = json.loads(cache_cycle)
                print(dict_cache_cycle)
                cache_cycle_delivered = int(dict_cache_cycle['DeliveredCount'])
                cache_cycle_failed = int(dict_cache_cycle['FailedDeliveredCount'])
                cache_cycle_notdelivered = int(dict_cache_cycle['NotDeliveredCount'])
                total_cycle = cache_cycle_delivered + cache_cycle_failed + cache_cycle_notdelivered
                print("********************")
                print(total_cycle)
                if total_cycle >= total_campaign:
                    #time_update =  datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                    time_update =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    update_total = "UPDATE cg_campaigns SET status='delivered', delivery_at='%s' WHERE id=%s" % (time_update, campaign_id)
                cursor.execute(update_total)
                mydb.commit()

        mydb.close()


    except Exception as e:
        # Manejar excepciones
        print(f"Error: {e}")

    finally:
        # Cerrar el cursor y la conexión
        cursor.close()
        mydb.close()


asyncio.run(main())
print("--- %s seconds ---" % (time.time() - start_time))

