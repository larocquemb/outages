#!/umesr/bin/python
import psycopg2
import psycopg2.extras
import sys
from pprint import pprint
import datetime
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from shapely.geometry import Polygon
from shapely import wkb
import time


def main():
    conn_string = "host='beaver' dbname='postgres' user='postgres' password='mysecretpassword'"
    # print the connection string we will use to connect
    print ("Connecting to database\n	->%s" % (conn_string))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    work_mem = 2048
    sql = ''
    cursor.execute('SET work_mem TO %s', (work_mem,))

    fs_url = 'https://services2.arcgis.com/QoeQkfdOG126FqSi/ArcGIS/rest/services/DetailedOutagesUTM/FeatureServer'
    outage_id = '05757b9722d444a6a55b126d18141d77'
    gis = GIS()
    event = {}
    outages = gis.content.get(outage_id)
    outage_layer = outages.layers[0]
    print(outage_layer.properties.capabilities)
    num_fields = len(outage_layer.properties.fields)
    query_results1 = outage_layer.query()
    num_attr = len(query_results1)
    dummytime = 153075891
    print ('Number of attributes = ', num_attr)
    for f in query_results1.features:
        print("$$$$$$$$$$$$$$$$$$$ new outage $$$$$$$$$$$$$$$$$$$$$$$")
        outage_id = f.attributes[outage_layer.properties.fields[8].name]
        if outage_id == "11111111":
            print("Dummy record", outage_id)
            continue
        for i in range(num_fields):
            print(outage_layer.properties.fields[i].name, f.attributes[outage_layer.properties.fields[i].name])
        objectid = f.attributes[outage_layer.properties.fields[0].name]
        if not objectid:
            objectid = 0
        iobjectid = int(objectid)
        outageid = f.attributes[outage_layer.properties.fields[1].name]
        if not outageid:
            outageid = 0
        ioutageid = int(outageid)
        numcustnopower = f.attributes[outage_layer.properties.fields[2].name]
        if not numcustnopower:
            numcustnopower = 0
        inumcustnopower = int(numcustnopower)
        time_of_outage = f.attributes[outage_layer.properties.fields[3].name]
        if not time_of_outage:
            time_of_outage = 0
        t1 = str(time_of_outage)
        t2 = t1[:10]
        t3 = int(t2)
        time_of_outage1 = datetime.datetime.utcfromtimestamp(t3)
        etr = f.attributes[outage_layer.properties.fields[4].name]
        if not etr:
            etr = 0
        e1 = str(etr)
        e2 = e1[:10]
        e3 = int(e2)
        etr1 = datetime.datetime.utcfromtimestamp(e3) 
        cause = f.attributes[outage_layer.properties.fields[5].name] 
        crew_status = f.attributes[outage_layer.properties.fields[6].name] 
        outage_type = f.attributes[outage_layer.properties.fields[7].name] 
        data_last_update = f.attributes[outage_layer.properties.fields[9].name] 
        if not data_last_update:
            data_last_update = 0
        d1 = str(data_last_update)
        d2 = d1[:10]
        d3 = int(d2)
        data_last_update1 = datetime.datetime.utcfromtimestamp(d3)
        num_cust_nopowertxt = f.attributes[outage_layer.properties.fields[10].name] 
        area = f.attributes[outage_layer.properties.fields[11].name] 
        field_verified_etr = f.attributes[outage_layer.properties.fields[12].name] 
        subcause = f.attributes[outage_layer.properties.fields[13].name] 
        poly10 = Polygon(f.geometry['rings'][0])
        polyhex = (poly10.wkb_hex)
        crs = 26914
        polydict = {'geom': poly10.wkb_hex, 'srid':crs}
        print("==================   casted types ===============")
        print("objectid", int(iobjectid))
        print("outageid", int(ioutageid))
        print("num_cust_nopower", int(inumcustnopower))
        print("time_of_outage", (time_of_outage1))
        print("etr", (etr1))
        print("cause", str(cause))
        print("crew_statue", str(crew_status))
        print("outage_type", str(outage_type))
        print("data_last_update", (data_last_update))
        print("num_cust_nopowertxt", str(num_cust_nopowertxt))
        print("area", float(area))
        print("field_verfiied_etr", str(field_verified_etr))
        print("subcause", str(subcause))
        print("geom", poly10)
        print("geom", (polyhex))
#
        cursor.execute("""
            INSERT INTO public.outages 
            (objectid, outageid, num_cust_nopower,time_of_outage,etr,cause,
            crew_status, outage_type, outage_id, data_last_update,num_cust_nopowertxt,
            area, field_verified_etr, subcause, geom)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, ST_GeomfromWKB(%s::geometry, 26914))
            """,
            (iobjectid, ioutageid, inumcustnopower, time_of_outage1, 
                etr1, cause, crew_status, outage_type, outage_id, 
                data_last_update1, num_cust_nopowertxt, 
                area, field_verified_etr, subcause, polyhex) )
        
    conn.commit()
    cursor.close()
 
if __name__ == "__main__":
    main()
    time.sleep(15*60)

