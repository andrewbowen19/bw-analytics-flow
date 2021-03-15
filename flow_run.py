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
import os
import time

today = (datetime.datetime.now()).date()

db_id = 'bluewireanalytics'
collection_id = 'simplecastdb'
database_link = 'dbs/' + 'bluewireanalytics'
collection_link = database_link + '/colls/' + 'bluewireanalytics'

config = {
    "endpoint": os.environ['AZURE_URI'],
    "primarykey": os.environ['AZURE_KEY']
}
print("COSMOS LINKS: ", config["endpoint"], config["primarykey"])
# Create the cosmos client
client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})

def podIDs():
    '''Get up-to-date pod IDs and titles from Simpelcast API'''
    pod_name_info = []
    dat = getSimplecastResponse('/podcasts?limit=1000')

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

def query_db_size():
    '''
    Check size of our db
    '''
    client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})
    database = client.get_database_client(db_id)
    container = database.get_container_client("simplecastdb")

    for item in container.query_items(
                query='SELECT VALUE COUNT(1) FROM c',
                enable_cross_partition_query=True):
        db_size = int(json.dumps(item, indent=True))
        return db_size

def clear_db():
    '''
    Clear db at start of new flow run
    '''
    client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})
    print('Clearing DB...')
    database = client.get_database_client(db_id)
    container = database.get_container_client(collection_id)
    for item in container.query_items(query='SELECT * FROM c',
                                  enable_cross_partition_query=True):
        container.delete_item(item, partition_key='podcastlevel')
        container.delete_item(item, partition_key='episodelevel')

    print('DB cleared!\n')

def update_network_level():
    '''
    Update network level doc
    '''
    client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})
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
                    "collection_podcasts": str([{'Title': p['title'], 'id': p['id']} for p in dat['collection']]),
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
    client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})
    # print(pod_ids)
    key_vaue = "podcastlevel"
    container_name = 'simplecastdb'

    log_path = os.path.join('.', 'logs', f'flow-run-{today}-log.txt')
    with open(log_path, 'w') as f:
        f.write(f'DB update executed: {today}\n')

        # Inserting data for each pod
        pods_updated = []
        for p in pod_ids:
            # Setting up db and container connection
            database = client.get_database_client(db_id)
            container = database.get_container_client(container_name)


            doc_id = str(uuid.uuid4())
            title = p['label']
            p_id = p['value']
            pods_updated.append(p_id)
            print(f'Getting Simplecast data for {title} | ({p_id})')
            episodes = getSimplecastResponse(f'/podcasts/{p_id}/episodes?limit=1000')
            downloads = getSimplecastResponse(f'/analytics/downloads?podcast={p_id}')
            listeners = getSimplecastResponse(f'/analytics/listeners?podcast={p_id}')

            data_dict = {"id": doc_id, "category": "podcastlevel",
                        "podcastid": p_id,
                        "collection_podcastanalytics": str(downloads['by_interval']),
                        "collection_podcastuniquelisteners" : str(listeners['by_interval']),
                        "collection_episodes": str(episodes['collection']),
                        "exportdate": today.strftime("%Y-%m-%d")}
            dict2string = json.dumps(data_dict)
            print(f'Inserting data in podcast level doc: {doc_id}')
            insert_data = container.upsert_item(data_dict)

            # Adding episode-level data
            episodes_updated = []
            for ep in episodes['collection']:
                database = client.get_database_client(db_id)
                container = database.get_container_client(container_name)
                ep_doc_id = str(uuid.uuid4())
                episode_id = ep['id']
                episodes_updated.append(episode_id)
                print(f'Uploading data for episode {episode_id}')
                ep_downloads = getSimplecastResponse(f'/analytics/downloads?episode={episode_id}')
                ep_listeners = getSimplecastResponse(f'/analytics/listeners?episode={episode_id}')
                
                if len(ep_listeners) > 0 and len(ep_downloads) > 0:
                    data_dict = {"id": ep_doc_id, "category": "episodelevel",
                            "episodeid": ep_doc_id,
                            "collection_episodeanalytics": str(ep_downloads['by_interval']),
                            "collection_episodeuniquelisteners" : str(ep_listeners['by_interval']),
                            "exportdate": today.strftime("%Y-%m-%d")}
                    container.upsert_item(data_dict)
                else:
                    print('No episodes detected, moving to next episode')

            print('Data inserted, on to the next!\n')
            print('################################')

            # Writing pod and episode ids to logs
            f.write('----------------------------------------')
            f.write(f'Podcasts upserted {pods_updated}\n')
            f.write(f'Podcasts upserted {episodes_updated}\n')


            time.sleep(10)
        


    
def update_episode_level():
    '''
    Update episode level data files
    '''
#    https://api.simplecast.com/analytics/downloads?episode={episode_id}
#    https://api.simplecast.com/analytics/listeners?episode=
    print('Updating episode-level data...')
#    ...
    client = cosmos_client.CosmosClient(url=config["endpoint"],
                                    credential={"masterKey":config["primarykey"]})

    key_vaue = "episodelevel"
    container_name = 'simplecastdb'
    for p in pod_ids:
        doc_id = str(uuid.uuid4())
        p_id = p['value']
        episodes = getSimplecastResponse(f'/podcasts/{p_id}/episodes?limit=1000')

        # print(episodes.keys())
        database = client.get_database_client(db_id)
        container = database.get_container_client(container_name)


        print('Data Added!\n')


    

# Run it, run it.
if __name__ =='__main__':
    pod_ids = podIDs()
    # update_network_level()
    db = client.get_database_client(db_id)
    print(db)
    db_size = query_db_size()
    print('DB SIZE: ', db_size)

    update_network_level()
    update_podcast_level()
    # update_episode_level()

# TODO:
    # Add episode-level upload func X
    # Clean code
    # Maybe rewrite as an object
    # Add env variables

