import glob
import json
import sys
from datetime import datetime

import psycopg2
from tqdm import tqdm
from nuts_finder import NutsFinder

nf = NutsFinder()

with open('data/api.json', 'r') as f:
    coord_map = json.load(f)

with open('data/station_sample.json') as f:
    station_sample = json.load(f)

stations = {}

plug_status_map = {
    'unbekannt': 'unknown',
    'frei': 'available',
    'besetzt': 'occupied',
    'reserviert': 'reserved',
    'au\u00DFer Betrieb': 'out_of_order'
}

plug_counter = 0
for station in station_sample:
    id = station['id']
    if str(id) in coord_map.keys():
        parts = station['address'].split(', ')
        street, zipcode = ', '.join(parts[:-1]).strip(), parts[-1]
        coord = coord_map[str(id)]
        # print(coord)
        nuts = nf.find(lat=coord['lat'], lon=coord['lng'])
        #print(nuts2)
        nuts1 = nuts[1]['NUTS_ID']
        nuts2 = nuts[2]['NUTS_ID']
        nuts3 = nuts[3]['NUTS_ID']
        nuts_str = ','.join([n['NUTS_ID'] for n in nuts[1:]])
        #nuts = [x for x in nuts2 if x['LEVL_CODE'] == 2][0]['NUTS_ID']
     
        plugs = []
        for plug in station['status']:
            plugs.append({
                'id': plug_counter,
                'type': plug['type'],
                'power': float(plug['power'].split(' ')[0]),
                'status': plug_status_map[plug['status']]
            })
            plug_counter += 1
        stations[id] = {
            'name': station['name'],
            'street': street,
            'zipcode': zipcode,
            'lat': coord['lat'],
            'lng': coord['lng'],
            'plugs': plugs,
            'nuts1': nuts1,
            'nuts2': nuts2,
            'nuts3': nuts3, 
        }


longest_street=max(map(lambda x: len(x['street']), stations.values()))
longest_name=max(map(lambda x: len(x['name']), stations.values()))
longest_zipcode=max(map(lambda x: len(x['zipcode']), stations.values()))


drop_statements = [
    """
    DROP TABLE IF EXISTS PlugStatus;
    """,
    """
    DROP TABLE IF EXISTS Plug;
    """,
    """
    DROP TABLE IF EXISTS Station;
    """,
    """
    DROP TYPE IF EXISTS status;
    """
]

create_statements = [

    f"""
    CREATE TABLE Station (
    	stationId SERIAL PRIMARY KEY,
    	name  VARCHAR({longest_name}),
    	street VARCHAR({longest_street}),
    	zipcode VARCHAR({longest_zipcode}),
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION,
        coord geography,
        NUTS1 VARCHAR(10),
        NUTS2 VARCHAR(10),
        NUTS3 VARCHAR(10)
    );
    """,
    """
    CREATE TYPE status AS ENUM ('unknown', 'available', 'occupied', 'reserved', 'out_of_order');
    """,
    """
    CREATE TABLE Plug (
    	plugId SERIAL PRIMARY KEY,
    	plugType VARCHAR(100) NOT NULL,
    	power NUMERIC(5, 1) NOT NULL,
    	stationId INTEGER REFERENCES Station(stationId) NOT NULL
    );
    """,
    """
    CREATE TABLE PlugStatus (
    	statusId SERIAL PRIMARY KEY,
    	timestamp TIMESTAMP NOT NULL,
        status status NOT NULL,
    	plugId INTEGER REFERENCES Plug(plugId)
    );
    """
]

with psycopg2.connect("dbname=postgres user=mihalkhan") as conn:
    with conn.cursor() as cur:
        # Execute a command: this creates a new table
        for statement in drop_statements:
            cur.execute(statement)

        for statement in create_statements:
            cur.execute(statement)

        print('inserting stations...')
        for id, station in tqdm(stations.items()):
            cur.execute(
                "INSERT INTO Station (stationId, name, street, zipcode, lat, lng, coord, NUTS1, NUTS2, NUTS3) VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326), %s, %s, %s)",
                (id, station['name'], station['street'], station['zipcode'], station['lat'], station['lng'], station['lng'], station['lat'], station['nuts1'], station['nuts2'], station['nuts3'] ))
    	    # plugId SERIAL PRIMARY KEY,
    	    # plugType VARCHAR(100),
    	    # power VARCHAR(100),
    	    # stationId INTEGER REFERENCES Station(stationId)
            for plug in station['plugs']:
                cur.execute(
                    "INSERT INTO Plug (plugId, plugType, power, stationId) VALUES (%s, %s, %s, %s)",
                    (plug['id'], plug['type'], plug['power'], id)
                )
        print('inserting timestamps...')
        for path in tqdm(glob.glob('all_data/*')):
            with open(path) as f:
                station_sample = json.load(f)
                for station in station_sample:
                    if station['id'] not in stations.keys():
                        continue
                    ref_station = stations[station['id']]
                    for status, plug in zip(station['status'], ref_station['plugs']):
    	                # statusId SERIAL PRIMARY KEY,
    	                # timestamp TIMESTAMPTZ NOT NULL,
    	                # plugId INTEGER REFERENCES Plug(plugId)
                        # datetime.fromtimestamp(int(path.split('/')[-1][:-5]))
                        cur.execute(
                            "INSERT INTO PlugStatus (timestamp, status, plugId) VALUES (%s, %s, %s)",
                            (datetime.fromtimestamp(station["timestamp"]/1000.0), plug_status_map[status['status']], plug['id'])
                        )

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no SQL injections!)
        # cur.execute(
        #     "INSERT INTO test (num, data) VALUES (%s, %s)",
        #     (100, "abc'def"))

        # Query the database and obtain data as Python objects.
        # cur.execute("SELECT * FROM test")
        # cur.fetchone()
        # will return (1, 100, "abc'def")

        # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
        # of several records, or even iterate on the cursor
        # for record in cur:
        #     print(record)

        # Make the changes to the database persistent
        conn.commit()

