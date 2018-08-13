
# coding: utf-8

# # SETTINGS:

# ## Code Controls

# In[1]:


#Choose the connection name, which is the name of the database for import to SQL
connection = "SF_OpenMapProject"

#Choose if you want to process parsing
## This will create the CSV files
process = True
overpassCSV = True

#Choose if you want to validate schema
## This will validate the structure and data type of the schema
'''
VALIDATING WILL MAKE THE PARSING PROCESS SLOWER (MORE THAN 10 SLOWER DEPENDING ON SYSTEM)
'''
validate_schema = False

#Choose if you want to create tables
## This will create the tables in SQLite
create = True

#Choose if you want to insert
## This will insert the data into SQLite tables
insert = True

#Chose if you want to drop tables after inserting to debug
## Create and re-insert tables after drop
drop_cond = False


# ## Importing Packages

# In[2]:


import xml.etree.cElementTree as ET
import cerberus
import schema
import re
import pprint
import csv
import codecs
import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import datetime as dt


# ## Filenames

# In[3]:


#Stating the name of the osm/xml file
OSM_FILE = "san-francisco_california.osm"

#Setting the csv file names and extention
NODES_FILE = "nodes.csv"
NODE_TAGS_FILE = "node_tags.csv"
WAYS_FILE = "ways.csv"
WAY_NODES_FILE = "way_nodes.csv"
WAY_TAGS_FILE = "way_tags.csv"
SCHEMA = schema.schema

#Setting the fields of the csv files
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 
           'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


# ## Problem Characters

# In[4]:


#Anly tag with these characters will be omitted
PROBLEMCHARS = re.compile(r'[=\+/&!=;\'"\?%#$@\,\. \t\r\n]')


# ## print(ing Function

# In[5]:


#s is the start time
def timing_print(s):
    print( '____________________')
    print( "Start Time: " + str(s))
    print( "Timing: " + str(dt.now() - s))
    print( "Conecttion: " + connection)
    print( '____________________')


# ***
# ***

# # PARSING DATA:

# ### Creating Generator

# In[6]:


def get_element(osm_file, tags=('node', 'way', 'relation')):
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()    


# ### Shaping Elements

# In[7]:


def shape_element(element):
    node_attribs = {} #dictionary
    way_attribs = {} #dictionary
    way_nodes = [] #list for way nodes
    tags = [] #list for ways and nodes
    position = 0 #position of way nodes
    
    #getting the unique identifier to set on tags
    child_id = element.attrib['id']
    #print( child_id
    
    ##############################
    # Getting Inside Node or Way #
    ##############################
    
    if element.tag == 'node':
        try:
            node_attribs['id'] = element.attrib['id']
            node_attribs['lat'] = element.attrib['lat']
            node_attribs['lon'] = element.attrib['lon']
            node_attribs['user'] = element.attrib['user']
            node_attribs['uid'] = element.attrib['uid']
            node_attribs['version'] = element.attrib['version']
            node_attribs['changeset'] = element.attrib['changeset']
            node_attribs['timestamp'] = element.attrib['timestamp']
            

        except Exception:
            node_attribs['id'] = element.attrib['id']
            node_attribs['lat'] = element.attrib['lat']
            node_attribs['lon'] = element.attrib['lon']
            node_attribs['user'] = 'No User'
            node_attribs['uid'] = 0
            node_attribs['version'] = element.attrib['version']
            node_attribs['changeset'] = element.attrib['changeset']
            node_attribs['timestamp'] = element.attrib['timestamp']
            
        #print( '___________')
        #print( element.tag)
        #print( node_attribs)

    elif element.tag == 'way':
        try:
            way_attribs['id'] = element.attrib['id']
            way_attribs['user'] = element.attrib['user']
            way_attribs['uid'] = element.attrib['uid']
            way_attribs['version'] = element.attrib['version']
            way_attribs['changeset'] = element.attrib['changeset']
            way_attribs['timestamp'] = element.attrib['timestamp']

        except Exception:
            way_attribs['id'] = element.attrib['id']
            way_attribs['user'] = 'No User'
            way_attribs['uid'] = 0
            way_attribs['version'] = element.attrib['version']
            way_attribs['changeset'] = element.attrib['changeset']
            way_attribs['timestamp'] = element.attrib['timestamp']
            
        #print( '___________')
        #print( element.tag)
        #print( way_attribs)
    
    ###################################################
    # Getting in between Start and End of Node or Way #
    ###################################################
    
    for child in element:
        

        if child.tag == 'tag': #for Tags Only
            
            #Gets information from tags
            child_tag_key = child.get('k')
            
            #Identify problematic tags
            key_problems = PROBLEMCHARS.search(child_tag_key)
            
            #Skips any tags with problems
            if key_problems != None:
                continue
                
            #Get the colon position in string
            colon_pos = child.get('k').find(':')
            
                        #Gets the value of the element
            value_string = child.attrib['v']
            
            
            if colon_pos > 0: #for Tags with colon only
                
                #Splitting keys inside tags
                key_tag = child.attrib['k'][colon_pos + 1:]
                type_tag = child.attrib['k'][0:colon_pos]
                
            else:
                
                #Set to '' to not add wiki language to key tag
                laguage_part = ''
                 
                #Ensuring that wikilanguage is with wiki
                if child.attrib['k'] == 'wikipedia':
                    
                    #Only does changes if colon is found
                    colon_pos = child.get('v').find(':')
                    laguage_part = ' in ' + child.attrib['v'][0:colon_pos]
                    value_string = child.attrib['v'][colon_pos + 1:]
                
                # Applying additions if needed and setting regular types
                key_tag = child.attrib['k'] + laguage_part
                type_tag = 'regular'
                
            #Changes to metadata when found
            #Cleaning wikipedia languages
            if type_tag == 'name':
                type_tag = 'language'
            
            #Cleaning address housenumber source
            elif type_tag == 'addr':
                if key_tag.find(':') > 0:
                    key_tag_array = key_tag.split(':')
                    if key_tag_array[0] == 'source':
                        sep = '_'
                        key_tag = sep.join([key_tag_array[1], key_tag_array[0]])
                    else:
                        key_tag = key_tag.replace(':', '_')   
                
            #Finishing tag touches into a dictionary
            node_tags = {}
            
            #Creating tag dictionaries
            node_tags['id'] = child_id
            node_tags['key'] = key_tag
            node_tags['value'] = value_string
            node_tags['type'] = type_tag
            
            #Appending node_tags into tag list
            tags.append(node_tags)
            
        else: #When it is not a tag
            
            #Finishing non-tag touches into dictionary
            nodes = {}
            
            #Creating non-tag dictionaries
            nodes['id'] = child_id
            nodes['node_id'] = child.get('ref')
            nodes['position'] = position
            
            #Incrementing the position of non-tags
            position += 1
            
            #Appending non-tags into way_nodes list
            way_nodes.append(nodes)
    
    #)( tags)
    #print( way_nodes)
    #print( '______')
    
    ############################
    # Returning Nodes and Ways #
    ############################
    if element.tag == 'node':
        #print( {'node': node_attribs, 'node_tags': tags})
        #print( '________________________________________')
         return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        #print( {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags})
        #print( '________________________________________')
         return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ### Validating Elements

# In[8]:


def validate_element(element, validator, schema=SCHEMA):
    #if there is a problem with the schema and data type
    if validator.validate(element, schema) is not True:
        
        #Gets the relative error and the field from the schema
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}'         has the following errors:\n{1}         on id: {2}"
        error_string = pprint.pformat(errors)
        
        #Gets the node id of the error to crosscheck
        child_id = element['node']['id']
        
        raise Exception(message_string.format(field, error_string, child_id))


# ### Writing the CSV file

# In[9]:


class UnicodeDictWriter(csv.DictWriter, object):
    
    # DEBUG HERE: what was the file doing in python 2
    def writerow(self, row):
        print(row.items())

        # python 2
        super(UnicodeDictWriter, self).writerow({k: (v.encode('utf-8') 
            if isinstance(v, unicode) else v) for k, v in row.iteritems()})

        # python 3
        super(UnicodeDictWriter, self).writerow({k: (v.encode('utf-8') 
            if isinstance(v, bytearray) else v) for k, v in row.items()})

    '''
    The fix from python 2 to 3 is here.
    .iteritems changed to just items
    https://stackoverflow.com/questions/30418481/error-dict-object-has-no-attribute-iteritems

    unicode has been changed to str
    https://stackoverflow.com/questions/36110598/nameerror-name-unicode-is-not-defined
    '''
    
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ### Processing Map

# In[10]:


def process_map(osm_file, validate):
    global part_message
    global end_message
    
    if validate:
        end_message = " validated. Please process the map."
    else:
        end_message = " processed. You can continue creation and importing."
    
    if not os.path.isfile('nodes.csv') or overpassCSV:
        print( "Beginning Process...")
        part_message = " has been"
        #Open an encoded file using the given mode and return 
        #a wrapped version providing transparent encoding/decoding
        #and set an alias
        #Creates the file
        with codecs.open(NODES_FILE, 'wb') as nodes_file,              codecs.open(NODE_TAGS_FILE, 'wb') as nodes_tags_file,              codecs.open(WAYS_FILE, 'wb') as ways_file,              codecs.open(WAY_NODES_FILE, 'wb') as way_nodes_file,              codecs.open(WAY_TAGS_FILE, 'wb') as way_tags_file:    

            #Setting Ojects
            nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
            node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
            ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
            way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
            way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

            #Create headers
            nodes_writer.writeheader()
            node_tags_writer.writeheader()
            ways_writer.writeheader()
            way_nodes_writer.writeheader()
            way_tags_writer.writeheader()

            #Validator used to check integrity of schema
            validator = cerberus.Validator()

            #Get into the yielded elements inside the osm file
            print( "Shaping & Writing Elements...")
            for element in get_element(osm_file):

                #Shape according to schema
                shape_el = shape_element(element)

                if shape_el:

                    #Validate the schema to ensure consistency
                    if validate:
                        validate_element(shape_el, validator)
                    
                    #Write rows of nodes or ways
                    if element.tag == 'node':
                        nodes_writer.writerow(shape_el['node'])
                        node_tags_writer.writerows(shape_el['node_tags'])
                    elif element.tag == 'way':
                        ways_writer.writerow(shape_el['way'])
                        way_nodes_writer.writerows(shape_el['way_nodes'])
                        way_tags_writer.writerows(shape_el['way_tags'])
    else:
        print( "The CSV file process has been done. Set overpassCSV to true to rewrite CSV files or to validate.")
        part_message = " was"

#Turn validate off to run the code 10x faster
if process:
    if validate_schema:
        print( "With Validation...")
    else:
        print( "Without Validation...")
    s = dt.now()
    process_map(OSM_FILE, validate=validate_schema)
    print( "The map" + part_message + end_message)
    timing_print(s)
else:
    print( "User opted not to process.")


# # CREATING AND IMPORTING DATA

# ###  Functions:

# In[13]:


#Run all queries under the parameter
def run_allqueries(queries):
    for i in range(len(queries)):
        try:
            c.execute(queries[i])
        except:
            print( 'Canceling execution. Please drop tables first.')


# In[11]:


#The funciton will be used to drop tables and debug
def drop_tables(drop_create, connection):
    
    if drop_create:
        db = sqlite3.connect(connection)
        c = db.cursor()
        query = '''
        DROP TABLE {};
        '''
        for i in range(len(queries_create)):
            Table_Name = queries_create[i][22:queries_create[i].find('(')]
            try:
                c.execute(query.format(Table_Name))
                print( 'Tables ' + Table_Name + ' drop.')
            except:
                print( 'Table ' + Table_Name + ' has ben droped already.')
        rows = c.fetchall()
        print( rows)
        db.close()
    else:
        print( "User opted to not drop tables.")


# ### Creating Tables

# In[14]:


#Creating Connection
db = sqlite3.connect(connection)
db.text_factory = str
c = db.cursor()

####################
# Creating Queries #
####################

#Creating List of Table Scripts
s = dt.now()
queries_create = ['''
        CREATE TABLE nodes(
            id integer PRIMARY KEY,
            lat real NOT NULL,
            lon real NOT NULL,
            user varchar(50) NOT NULL,
            uid integer NOT NULL,
            version integer NOT NULL,
            changeset integer NOT NULL,
            timestamp varchar(25) NOT NULL
        );
        ''',
        '''
        CREATE TABLE node_tags(
            nodeid integer NOT NULL,
            key varchar(50),
            value varchar(300),
            type varchar(50),
            FOREIGN KEY(nodeid) REFERENCES nodes(id)
        );
        ''',
        '''
        CREATE TABLE ways(
            id integer PRIMARY KEY,
            user varchar(50) NOT NULL,
            uid integer NOT NULL,
            version integer NOT NULL,
            changeset integer NOT NULL,
            timestamp varchar(25) NOT NULL
        );
        ''',
        '''
        CREATE TABLE way_tags(
            wayid integer NOT NULL,
            key varchar(50),
            value varchar(300),
            type varchar(50),
            FOREIGN KEY(wayid) REFERENCES ways(id)
        );
        ''',
        '''
        CREATE TABLE way_nodes(
            wayid integer NOT NULL,
            node_id integer NOT NULL,
            position integer NOT NULL,
            FOREIGN KEY(wayid) REFERENCES ways(id)
        );
        '''
        ]

#Creating All Tables
if create:
    print( "Creating tables...")
    run_allqueries(queries_create)
    timing_print(s)
else:
    print( "User opted not to create tables.")

####################
# Inserting Values #
####################

if insert:
    #Creatinf Insert Scripts
    print( "Inserting data:")
    s = dt.now()
    queries_insert = {
        'nodes':
        '''
        INSERT INTO nodes   
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        ''',
        'node_tags':
        '''
        INSERT INTO node_tags
        VALUES (?, ?, ?, ?);
        ''',
        'ways':
        '''
        INSERT INTO ways
        VALUES (?, ?, ?, ?, ?, ?);
        ''',
        'way_tags':
        '''
        INSERT INTO way_tags
        VALUES (?, ?, ?, ?);
        ''',
        'way_nodes':
        '''
        INSERT INTO way_nodes
        VALUES (?, ?, ?);
        '''
        }
    
    with codecs.open(NODES_FILE, 'rb') as nodes,          codecs.open(NODE_TAGS_FILE, 'rb') as node_tags,          codecs.open(WAYS_FILE, 'rb') as ways,          codecs.open(WAY_TAGS_FILE, 'rb') as way_tags,          codecs.open(WAY_NODES_FILE, 'rb') as way_nodes:

        #Create an object of values into a dict
        nodes_dr = csv.DictReader(nodes)
        node_tags_dr = csv.DictReader(node_tags)
        ways_dr = csv.DictReader(ways)
        way_tags_dr = csv.DictReader(way_tags)
        way_nodes_dr = csv.DictReader(way_nodes)

        #Inserting nodes values
        print( 'Processing nodes...')
        for i in nodes_dr:
            c.execute(queries_insert['nodes'], 
            (i['id'], i['lat'], i['lon'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']))

        print( 'Processing node tags...')
        #Inserting node_tags values
        for i in node_tags_dr:
            c.execute(queries_insert['node_tags'], 
            (i['id'], i['key'], i['value'], i['type']))

        print( 'Processing way...')
        #Inserting way values
        for i in ways_dr:
            c.execute(queries_insert['ways'], 
            (i['id'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']))


        print( 'Processing way tags...')
        #Inserting way_tags values
        for i in way_tags_dr:
            c.execute(queries_insert['way_tags'], 
            (i['id'], i['key'], i['value'], i['type']))

        print( 'Processing way nodes...')
        #Inserting way_nodes values
        for i in way_nodes_dr:
            c.execute(queries_insert['way_nodes'], 
            (i['id'], i['node_id'], i['position']))

        #Completion message
        print( 'Table Creation and Value Insert Process completed!')
        timing_print(s)
else:
    print( "User opted not to insert data.")
    
db.commit()
db.close()


# ###  Dropping Database if Needed

# In[15]:


#Drop tables to recreate database while debuging
drop_tables(drop_cond, connection)


# ## Connecting for Data Analysis

# In[16]:


#Starting connection to server
db = sqlite3.connect(connection)
c = db.cursor()


# ## Verifying Completeness

# In[17]:


#XML Count Section
xmlnode_count = 0
xmlnodetags_count = 0
xmlway_count = 0
xmlwaynodes_count = 0
xmlwaytags_count = 0
node_omitted_ids = []
way_omitted_ids = []
node_omitted_tags = []
way_omitted_tags = []
node_omitted_value = []
way_omitted_value = []


#Starting Point
s = dt.now()

print( "Processing XML...")

#XML iteration
for element in get_element(OSM_FILE):
    
    if element.tag == 'node':
        xmlnode_count += 1
        
        for child in element:
            if child.tag == 'tag':
                
                #Gets information from tags
                child_tag_key = child.get('k')
                
                #Identify problematic tags
                key_problems = PROBLEMCHARS.search(child_tag_key)
                
                if key_problems != None:
                    
                    #Get list of omitted ids and tags for reference
                    node_omitted_ids.append(element.attrib['id'])
                    node_omitted_tags.append(child.get('k'))  
                    node_omitted_value.append(child.get('v'))
                    
                else:
                    xmlnodetags_count += 1            
                
    elif element.tag == 'way':
        xmlway_count += 1
        for child in element:
            #print( child.tag
            if child.tag == 'tag':
                                
                #Gets information from tags
                child_tag_key = child.get('k')
                
                #Identify problematic tags
                key_problems = PROBLEMCHARS.search(child_tag_key)
                
                if key_problems != None:
                    
                    #Get list of omitted ids and tags for reference
                    way_omitted_ids.append(element.attrib['id'])
                    way_omitted_tags.append(child.get('k'))
                    way_omitted_value.append(child.get('v'))
                else:
                    xmlwaytags_count += 1
                    
            elif child.tag == 'nd':
                xmlwaynodes_count +=1

#List of dictionaries of omitted tags of ways and nodes
omitted_dict = [{'way_id': way_omitted_ids, 'way_tags': way_omitted_tags, 'way_value': way_omitted_value},                {'node_id': node_omitted_ids, 'node_tags': node_omitted_tags, 'node_value': node_omitted_value}]
                
#CSV Count Section
CSV_FILES = ("nodes.csv","node_tags.csv",             "ways.csv","way_nodes.csv","way_tags.csv") 
print( "Processing CSV...")

#CSV iteration
for csv_file in CSV_FILES:
    csv_count = -1
    with open(csv_file, 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            i_num = CSV_FILES.index(csv_file)
            csv_count += 1
            if i_num == 0:
                csvnode_count = csv_count
            elif i_num == 1:
                csvnodetags_count = csv_count
            elif i_num == 2:
                csvway_count = csv_count
            elif i_num == 3:
                csvwaynodes_count = csv_count
            elif i_num == 4:
                csvwaytags_count = csv_count
        
#SQLite Count Section
table_queries = '''
SELECT COUNT(*)
FROM {}
'''

print( "Processing SQLite...")
tables_count = {}

#SQLite iteration
for csv_file in CSV_FILES:
    table_name = csv_file.split(".")[0]
    tables_count[table_name]= np.array(pd.read_sql_query(table_queries.format(table_name), db))[0][0]

print( "________________________________________________")

print( "There are " + str(xmlnode_count) + " node records in the XML file.")
if xmlnode_count == csvnode_count == tables_count['nodes']:
    print( "Completion: Validated")
else:
    print( "Completion: Invalid")
print( "There are " + str(xmlnodetags_count) + " node_tags records in the XML file.")
if xmlnodetags_count == csvnodetags_count == tables_count['node_tags']:
    print( "Completion: Validated")
else:
    print( "Completion: Invalid")
print( "There are " + str(xmlway_count) + " way records in the XML file.")
if xmlway_count == csvway_count == tables_count['ways']:
    print( "Completion: Validated")
else:
    print( "Completion: Invalid")
print( "There are " + str(xmlwaytags_count) + " way_tags records in the XML file, " + str(len(node_omitted_ids)) + " were omitted.")
if xmlwaytags_count == csvwaytags_count == tables_count['way_tags']:
    print( "Completion: Validated")
else:
    print( "Completion: Invalid")
print( "There are " + str(xmlwaynodes_count) + " way_nodes records in the XML file, " + str(len(way_omitted_ids)) + " were omitted.")
if xmlwaynodes_count == csvwaynodes_count == tables_count['way_nodes']:
    print( "Completion: Validated")
else:
    print( "Completion: Invalid"  ) 
timing_print(s)


# ### Validating Changes

# ### List of Key Tags 

# In[18]:


address_keys = '''
SELECT DISTINCT key
FROM node_tags
WHERE type = 'addr';
'''
print( pd.read_sql_query(address_keys, db))


# ### Housenumber Description

# In[19]:


dig_on_address = '''
SELECT nodeid, type, key, value
FROM node_tags
WHERE key IN ('source:housenumber', 'housenumber:source', 'housenumber_source');
'''
print( pd.read_sql_query(dig_on_address, db))


# ### Type Language and Description

# In[20]:


#This query shows what the type name is describing
type_name_desc = '''
SELECT
    nodeid,
    type,
    key,
    value
FROM node_tags
WHERE LOWER(type) = 'language'
LIMIT 16 --limiting to save space
'''
print( 'Type Name Description')
s = dt.now()
print( pd.read_sql_query(type_name_desc, db))
timing_print(s)


# ## Statistics

# ### Number of Nodes

# In[21]:


table_names = ('nodes', 'node_tags', 'ways', 'way_tags', 'way_nodes')

number_nodes_query = '''
SELECT COUNT(*) AS {table}_count
FROM {table};
'''

s = dt.now()
for table in table_names:
    print( pd.read_sql_query(number_nodes_query.format(table=table), db))
timing_print(s)


# ### Number of Unique Users

# In[22]:


tables_user = ('nodes', 'ways')

number_uniq_user = '''
SELECT COUNT(DISTINCT uid) AS unique_{name}_count
FROM ({table})
'''

subquery = '''
SELECT uid
FROM nodes
UNION ALL
SELECT uid
FROM ways
'''

s = dt.now()
for table in tables_user:
    print( pd.read_sql_query(number_uniq_user.format(table=table, name=table), db))
print( pd.read_sql_query(number_uniq_user.format(table=subquery, name='total'), db))
timing_print(s)


# ### Top 10 Most Contributing Users

# In[23]:


number_uniq_user = '''
SELECT user, COUNT(*) AS contrib_count
FROM (SELECT uid, user FROM nodes UNION ALL SELECT uid, user FROM ways)
GROUP BY uid
ORDER BY COUNT(*) DESC
LIMIT 10;
'''
s = dt.now()
print( pd.read_sql_query(number_uniq_user, db))
timing_print(s)


# ### Top type entries with over 100

# In[24]:


#This query will select the type an its count
bytype_count = '''
SELECT type, COUNT(*) AS TypeCount
FROM node_tags
GROUP BY type
HAVING COUNT(*) > 100
ORDER BY COUNT(*) DESC
'''
s = dt.now()
print( pd.read_sql_query(bytype_count, db))
timing_print(s)


# ### Number of tags that need special attention by source node and type over total count of tags

# In[25]:


validate_total_count = '''
SELECT COUNT(*) AS way_regular_total
FROM way_tags
WHERE type = 'regular' and LOWER(key) != 'fixme'
'''
validate_fixme_count = '''
SELECT 
    --w1.wayid, --commented after crosschecking with source
    COUNT(w2.wayid) AS way_regular_fixme
FROM
(SELECT wayid FROM way_tags w1 WHERE type = 'regular' and LOWER(key) != 'fixme') w1
LEFT JOIN 
(SELECT DISTINCT wayid FROM way_tags WHERE LOWER(key) = 'fixme') w2
ON w1.wayid = w2.wayid
--WHERE w1.wayid = 156124086 --commented after crosschecking
--GROUP BY w1.wayid --commented after crosschecking with source
'''
fixme_tags = '''
SELECT source_node, type, COUNT(fixid) AS fixme_count, COUNT(*) AS total_count
FROM
(
SELECT * FROM (
(SELECT 'node' AS source_node, nodeid as id, type, key, value FROM node_tags WHERE LOWER(key) != 'fixme') node_nofix
LEFT JOIN
(SELECT nodeid as fixid FROM node_tags WHERE LOWER(key) = 'fixme') node_fix
ON node_nofix.id = node_fix.fixid)
UNION
SELECT * FROM (
(SELECT 'way' AS source_node, wayid as id, type, key, value FROM way_tags WHERE LOWER(key) != 'fixme') way_nofix
LEFT JOIN
(SELECT wayid as fixid FROM way_tags WHERE LOWER(key) = 'fixme') AS way_fix
ON way_nofix.id = way_fix.fixid)
)
GROUP BY source_node, type
HAVING COUNT(fixid) > 0
ORDER BY COUNT(fixid) DESC;
'''
s = dt.now()
print( pd.read_sql_query(validate_total_count, db))
print( pd.read_sql_query(validate_fixme_count, db))
print( pd.read_sql_query(fixme_tags, db))
timing_print(s)


# # Aditional Ideas

# ### Adress Format Parts

# In[26]:


address_parts = '''
SELECT key, COUNT(*) AS addr_part_count
FROM
(SELECT * FROM node_tags
UNION ALL
SELECT * FROM way_tags)
WHERE type = 'addr'
GROUP BY key
ORDER BY COUNT(*) DESC;
'''
s = dt.now()
print( pd.read_sql_query(address_parts, db))
timing_print(s)


# ### Discovering Full Type

# In[27]:


examples_addr = '''
SELECT nodeid, value
FROM node_tags
WHERE key = 'full'
LIMIT 10
'''
print( pd.read_sql_query(examples_addr, db))


# ### Testing Numeric Conditional

# In[28]:


testing = '''
SELECT nodeid, value, cast(value as integer) > 0
FROM node_tags
WHERE key = 'housename' and typeof(cast(value as integer)) != 'text' and nodeid = 358772135 and (cast(value as integer) > 0) = 1
LIMIT 100
'''

print( pd.read_sql_query(testing, db))


# ### Important Address Parts Query

# In[29]:


important_addr_parts = '''
SELECT id, lat || ', ' || lon AS googlemap_checker, housenumber, housename, COALESCE(housenumber, housename) AS number, street, unit, state, country, postcode
FROM nodes main
LEFT JOIN
(SELECT nodeid, value AS housenumber FROM node_tags WHERE key = 'housenumber') AS hnum
ON hnum.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS housename FROM node_tags WHERE key = 'housename' and (cast(value as integer) > 0) = 1) AS hnam
ON hnam.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS street FROM node_tags WHERE key = 'street') AS str
ON str.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS unit FROM node_tags WHERE key = 'unit') AS un
ON un.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS state FROM node_tags WHERE key = 'state') AS sta
ON sta.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS country FROM node_tags WHERE key = 'country') AS coun
ON coun.nodeid = main.id
LEFT JOIN
(SELECT nodeid, value AS postcode FROM node_tags WHERE key = 'postcode') AS post
ON post.nodeid = main.id
WHERE housenumber IS NULL and housename IS NOT NULL
--LIMIT 10
'''
print( pd.read_sql_query(important_addr_parts, db))


# ### Understanding Keys Query

# In[30]:


#This query give us the top 20 keys and their number of entries
bykey_count = '''
SELECT 
    key,
    COUNT(*) AS KeyCount
FROM node_tags
GROUP BY key
HAVING COUNT(*) > {ghaving}
ORDER BY COUNT(*) {order}
LIMIT {limit} --limiting to save space
'''

print( 'Number of Keys')
s = dt.now()
print( pd.read_sql_query(bykey_count.format(order='DESC', ghaving=0, limit=10), db))
timing_print(s)
print( 'Looking for More Important Keys')
s = dt.now()
print( pd.read_sql_query(bykey_count.format(order='', ghaving=30, limit=40), db))
timing_print(s)


# ### Key Name Description Query

# In[31]:


#This query describes the key names
key_name_desc = '''
SELECT
    nodeid,
    type,
    key,
    value AS PlaceName
FROM node_tags
WHERE LOWER(key) = 'name'
LIMIT 20 --limiting to save space
'''

print( '')
print( 'Key Name Description')
s = dt.now()
print( pd.read_sql_query(key_name_desc, db))
timing_print(s)


# ### Digging into an Example

# In[32]:


#This query will give us more information about the key name by nodeid
expanded_type_id = '''
SELECT 
    nodeid,
    type,
    key,
    substr(value, 1, 25) AS value
FROM node_tags
WHERE nodeid = 26819236
'''

print( 'Expanded details of tag for id = 26819236')
s = dt.now()
print( pd.read_sql_query(expanded_type_id, db))
timing_print(s)


# ### Investigating wikipedia language

# In[33]:


Unsplited_Query = '''
SELECT nodeid, type, key, value
FROM node_tags
WHERE value LIKE '%:%'
    AND key IN ('wikipedia') --change to identify trend by key;
'''
print( pd.read_sql_query(Unsplited_Query, db))

Group_byKey = '''
SELECT key, COUNT(*) AS keycount
FROM ({subquery})
GROUP BY key
ORDER BY COUNT(*) DESC
LIMIT 30;
'''

print( pd.read_sql_query(Group_byKey.format(subquery=Unsplited_Query), db))


# ### How Many Names Have Fixme Tags

# In[34]:


names_fixme = '''
--Count of names with fixme tags
SELECT COUNT(*) AS namefixme_count
FROM node_tags as main
JOIN (SELECT nodeid FROM node_tags WHERE LOWER(key) = 'fixme') AS fixme
ON main.nodeid = fixme.nodeid
WHERE LOWER(key) = 'name';
'''
print( pd.read_sql_query(names_fixme, db))


# ### Creating Query to Assess for Accuracy of Fixme PlaceTypes

# In[40]:


#This query self join the node_tags table to bring several tags under one new category.
#create temp table search as select * from documents
namedplace_issue = '''
CREATE TEMP TABLE fixme AS SELECT *
FROM
(
SELECT 
    id,
    name.value AS PlaceName,
    CASE
        WHEN LOWER(name.value) LIKE '%caltran%'THEN 'Caltrans'
        WHEN LOWER(name.value) LIKE '%trail%' THEN 'Trail'
        WHEN LOWER(name.value) LIKE '%association%' THEN 'Association'
        WHEN LOWER(name.value) LIKE '%center%' THEN 'Center'
        WHEN LOWER(name.value) LIKE '%entrance%' THEN 'Entrance'
        ELSE COALESCE(place.value, amen.value, high.value, misc.value, tour.value, waytag.value)
    END AS PlaceType,
    COALESCE(addr.value, waytag.value) AS Address,
    fixme.value AS FixMessage,
    lat || ', ' || lon AS googlemap_checker
FROM nodes n 
JOIN --left to include those w/o names
(SELECT nodeid, type, key, value FROM node_tags WHERE key = 'name') name
ON n.id = name.nodeid
LEFT JOIN --left to include those w/o places
(SELECT nodeid, value FROM node_tags WHERE key = 'place') place
ON n.id = place.nodeid
LEFT JOIN --leff to include those w/o amenities
(SELECT nodeid, value FROM node_tags WHERE key = 'amenity') amen
ON n.id = amen.nodeid
LEFT JOIN --leff to include those w/o highway
(SELECT nodeid, value FROM node_tags WHERE key = 'highway') high
ON n.id = high.nodeid
LEFT JOIN --leff to include those w/o highway
(SELECT nodeid, 
CASE WHEN LOWER(key) = 'shop' THEN 'Shop' ELSE value END AS value FROM node_tags 
WHERE (LOWER(key) = 'shop' and value = 'yes') OR (LOWER(key) = 'entrance' and value = 'yes')) misc
ON n.id = misc.nodeid
LEFT JOIN --left to include those w/o address
(SELECT nodeid, group_concat(value) AS value FROM node_tags WHERE type = 'addr' GROUP BY nodeid) addr
ON n.id = addr.nodeid
LEFT JOIN --leff to include those w/o highway
(SELECT nodeid, value FROM node_tags WHERE LOWER(key) = 'tourism') tour
ON n.id = tour.nodeid
JOIN --left to force only those w/ potential issues
(SELECT nodeid, value FROM node_tags WHERE LOWER(key) = 'fixme') fixme
ON n.id = fixme.nodeid
LEFT JOIN
(SELECT DISTINCT node_id, key, 
CASE WHEN LOWER(key) = 'building' THEN 'Building' ELSE value END AS Value FROM way_nodes wn
JOIN way_tags wt ON wn.wayid = wt.wayid WHERE LOWER(key) = 'waterway' OR LOWER(key) = 'building') waytag 
--adds more tag descriptions but slows down the query
ON n.id = waytag.node_id
);
'''
s = dt.now()
try:
    c.execute(namedplace_issue)
    db.commit()
except:
    print( 'Temp Table Exist')
    
timing_print(s)


# In[41]:


#Set to true to drop temporary table above
drop_temp = False
drop_tem_query = '''
drop table fixme
'''

if drop_temp:
    c.execute(drop_tem_query)
    db.commit()


# ### 6.3 Validating Complex Fixme Query

# In[42]:


#This query will validate the query above
validating_complex_q = '''
SELECT 
    COUNT(*) AS ValidCount
FROM fixme;
'''

#type 'LEFT' if you want to see all errors, type '' if not
#example .format('LEFT') or .format('')
s = dt.now()
print( 'Fixme Count on Complex Query')
print( pd.read_sql_query(validating_complex_q.format(sq=namedplace_issue), db))
timing_print(s)


# ### Count of Fixme nodes by Place Type

# In[43]:


#This query count the number of recrods under the new PlaceType metadata
placetype_count = '''
SELECT 
    PlaceType,
    COUNT(*) AS FixmeCount
FROM fixme
GROUP BY PlaceType
ORDER BY COUNT(*) DESC;
'''

#type 'LEFT' if you want to see all errors, type '' if not
#example .format('LEFT') or .format('')
s = dt.now()
print( 'Number of Fixme Claims by PlaceType Including No-name Places to Validate')
print( pd.read_sql_query(placetype_count.format(sq=namedplace_issue), db))
timing_print(s)


# ### List of Place Names, Place Type and Address

# In[44]:


#This query selects specific columns uder the newly created table in section 5.2
accuracy_test = '''
SELECT
    id AS nodeid,
    PlaceName,
    PlaceType,
    googlemap_checker
FROM fixme;
'''
s = dt.now()
print( pd.read_sql_query(accuracy_test, db))
timing_print(s)


# ### Creating Tables and Inserting Findings

# In[45]:


#Table to validate for accuracy of Place Type
temp_valid = '''
CREATE TEMP TABLE PlaceTypeValid (
    id INT PRIMARY KEY,
    valid INT,
    googlefinding VARCHAR(75)
)
'''

#Create table if not exist
try:
    c.execute(temp_valid)
    print( 'Table created.')
except:
    print( 'Table already created.')
    
#Assessment
# 1 when valid, 0 when invalid
insert_validation = '''
INSERT INTO PlaceTypeValid
VALUES 
('4339548347',1, 'It is a contemporary pâtisserie'),
('3474438154',1 ,'Deli, Sandwich, Coffee and Tea, Bagels shop'),
('3228601037',1 ,'It is a nonprofit counseling center'),
('3069400840',1 ,'Mexican Cuisine'),
('2765693662',0 ,'Undetermined, more info needed'),
('2639227318',1 ,'Ice cream shop selling gelato'),
('2525079629',1 ,'Waterway stream in Berkerly'),
('2356902019',1 ,'A mental health association'),
('2217251865',1 ,'A trail in the coyote hills regional park'),
('2217250747',1 ,'A trail in the coyote hills regional park'),
('1924019363',0 ,'Frozen Yougurt not really representing a restaurant'),
('1901868592',1 ,'A hotel in the city'),
('1210822824',1 ,'A toll Place by the Interstate 80'),
('1184118036',0 ,'Undetermined, more data needed'),
('1110125147',0 ,'Undetermined, more data needed'),
('1110102182',0 ,'Undetermined, more data needed'),
('816300001',0 ,'Undetermined, more data needed'),
('816289382',0 ,'Undetermined, more data needed'),
('368170033',0 ,'Undetermined, more data needed'),
('282600552',1 ,'A church'),
('26819236',1 ,'San Francisco the City')
'''

#Insert data if not there
try:
    c.execute(insert_validation)
    print( 'Data inserted.')
except:
    print( 'Data already inserted.')


# ### List of nodes and findings

# In[47]:


list_w_finding = '''
SELECT main.id, placename, placetype, valid, googlefinding
FROM fixme AS main
JOIN placetypevalid AS ptv
ON main.id = ptv.id;
'''
s = dt.now()
print( pd.read_sql_query(list_w_finding, db))
timing_print(s)


# ### Count of User Concerns by Rank

# In[49]:


total_issue = '''
SELECT
    COUNT(*) AS NumberIssues
FROM node_tags
WHERE LOWER(key) = 'fixme'
'''

Issue_count_rank = '''
WITH IssueTable AS 
(
SELECT 
    value,
    COUNT(*) AS IssueCount
FROM node_tags nt
LEFT JOIN nodes n
ON nt.nodeid = n.id
WHERE LOWER(key) = 'fixme'
GROUP BY value
ORDER BY COUNT(*) DESC
)
SELECT 
    *,
    (
    SELECT
        COUNT(*) + 1
    FROM 
        (
        SELECT DISTINCT IssueCount
        FROM IssueTable i1
        WHERE main.IssueCount < i1.IssueCount
        )
    ) AS IssueRank
FROM IssueTable main
'''

print( 'Fixme Messages')
s = dt.now()
print( pd.read_sql_query(total_issue, db))
print( 'Reported Error Message Count and Rank')
print( pd.read_sql_query(Issue_count_rank, db))
timing_print(s)

