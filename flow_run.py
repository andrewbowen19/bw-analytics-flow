# Script to insert data into our cosmos db

import pandas as pd
import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents
import azure.cosmos.http_constants as http_constants
import http.client
import datetime
import uuid

today = (datetime.datetime.now()).date()

db_id = 'bluewireanalytics'
collection_id = 'simplecastdb'
database_link = 'dbs/' + 'bluewireanalytics'
collection_link = database_link + '/colls/' + 'bluewireanalytics'

config = {
    "endpoint": "https://bluewireanalytics.documents.azure.com:443/",
    "primarykey": "4hfyctggjnhdsiVl6UVIzAkGWrlHtgv32yAU8hZMRPjCuoqNIPmBBpMyCnFrqmgt3nD7nmfKbrHFuZIL9Wc9SA=="
}

# Create the cosmos client
client = cosmos_client.CosmosClient(url=config["endpoint"], credential={"masterKey":config["primarykey"]}
)


def podIDs():
    '''Get up-to-date pod IDs and titles from Simpelcast API'''
    pod_name_info = []
    dat = getSimplecastResponse('/podcasts?limit=1000')
    print(dat)

    # Writing API title-id responses to list
    for item in dat['collection']:
        pod_name_info.append({'label': item['title'], 'value': item['id']})

    return pod_name_info

def getSimplecastResponse(query_params):
    '''
    Method to establish connection to Simplecast API
    query_params - string for HTTP request params
    Check Simplecast docs for this
    '''
    # Figure out how to set this as an env variable
    auth = 'Bearer eyJhcGlfa2V5IjoiMmY4MThhMDg3NzEyOTYxZTk3NzcwNTM3NDJjMmJiNmUifQ=='
    payload = ''
    url = "api.simplecast.com"
    headers = {'authorization': auth}
    conn = http.client.HTTPSConnection(url)
    conn.request("GET", query_params, payload, headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()

    return json.loads(data.decode('utf-8')) #str

def clear_db():
    '''
    Clear db at start of new flow run
    '''
    print('Clearing DB...')
    database = client.get_database_client(db_id)
    container = database.get_container_client(collection_id)
    for item in container.query_items(query='SELECT * FROM c',
                                  enable_cross_partition_query=True):
        print(type(item))
        container.delete_item(item, partition_key='networklevel')
        container.delete_item(item, partition_key='podcastlevel')
    print('DB cleared!\n')

def update_network_level():
    '''
    Update network level doc
    '''
    dat = getSimplecastResponse('/podcasts?limit=1000')
    print(dat)

    key_vaue = "networklevel"
    network_id = '3dce1d89-c561-4cbe-bf25-1595f6fc40c7'
    activity_id = 'eaf521bb-5d3e-407c-86a4-8fd628093f0a'

    
    container_name = 'simplecastdb'
    database = client.get_database_client(db_id)
    container = database.get_container_client(container_name)

    print(f'Container & DB: {container}, {database}')
    data_dict = {"id": network_id, "category": "networklevel",
                    "collection_podcasts": dat['collection'],
                    "exportdate": today.strftime("%Y-%m-%d")}

    dict2string = json.dumps(data_dict)
    print(dict2string, type(dict2string))
    # Insert me baby
    insert_data = container.upsert_item(data_dict)


    print('Network Records inserted successfully.')

def update_podcast_level():
    '''
    Update podcast level data files
    '''
    print('Updating podcast-level data...')
#    https://api.simplecast.com/podcasts/@{<pod_id>}/episodes?limit=1000 -- Get episodes by pod
#    https://api.simplecast.com/analytics/downloads?podcast={pod_id} -- Get podcast downloads
#    https://api.simplecast.com/analytics/listeners?podcast=(pod_id) -- get pod unique listeners
#    ...
    pod_ids = podIDs()
    # print(pod_ids)
    key_vaue = "podcastlevel"
    container_name = 'simplecastdb'


    for p in pod_ids:
        # Setting up db and container connection
        database = client.get_database_client(db_id)
        container = database.get_container_client(container_name)


        doc_id = str(uuid.uuid4())
        title = p['label']
        p_id = p['value']
        print(f'Getting Simplecast data for {title} | ({p_id})')
        episodes = getSimplecastResponse(f'/podcasts/{p_id}/episodes?limit=1000')
        downloads = getSimplecastResponse(f'/analytics/downloads?podcast={p_id}')
        listeners = getSimplecastResponse(f'/analytics/listeners?podcast={p_id}')

        data_dict = {"id": doc_id, "category": "podcastlevel",
                    "podcastid": p_id,
                    "collection_podcastanalytics": str(downloads['by_interval']),
                    "collection_podcastuniquelisteners" : str(listeners['by_interval']),
                    "collection_episodes": str(pisodes['collection']),
                    "exportdate": today.strftime("%Y-%m-%d")}
        dict2string = json.dumps(data_dict)
        print(f'Inserting data in doc ID {doc_id}')
        insert_data = container.upsert_item(data_dict)

        print('Data inserted, on to the next!\n')
        print('###################')


    
def update_episode_level():
    '''
    Update episode level data files
    '''
#    https://api.simplecast.com/analytics/downloads?episode=
#    https://api.simplecast.com/analytics/listeners?episode=
    print('Updating episode-level data...')
#    ...



if __name__ =='__main__':
    # update_network_level()
    clear_db()
    # update_network_level()
    # update_podcast_level()



