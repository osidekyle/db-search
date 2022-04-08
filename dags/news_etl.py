import urllib.request
import json
import xml.etree.ElementTree as ET
from cassandra.cluster import Cluster
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def get_news_data():
    opener = urllib.request.FancyURLopener()
    url = "https://feeds.npr.org/1001/rss.xml"
    f = opener.open(url)
    content = f.read()

    root = ET.fromstring(content)
    stories = root.findall("./channel/item")

    storyList = []

    for story in stories:
        storyMap = {}
        storyMap['title'] = story.find("./title").text
        storyMap['description'] = story.find("./description").text
        storyMap['pubDate'] = story.find("./pubDate").text
        storyMap['guid'] = story.find("./guid").text
        storyList.append(storyMap)
        print(storyMap, "\n")

    
    cluster = Cluster(["cassandra"], port=9042)
    session = cluster.connect()
    keyspaces = session.execute("describe keyspaces")
    for keyspace in keyspaces:
        print(keyspace)
    session.execute('CREATE KEYSPACE IF NOT EXISTS news WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\' : 3};')
    keyspaces = session.execute("describe keyspaces")
    for keyspace in keyspaces:
        print(keyspace)

    
    session.execute('USE news')
    session.execute('CREATE TABLE IF NOT EXISTS news_data(guid text PRIMARY KEY,title text,description text,pubDate text);')
   

    
    for story in storyList:
        session.execute("""INSERT INTO news_data (guid, title, description, pubDate) VALUES(%s, %s, %s, %s);""",(story['guid'], story['title'], story['description'], story['pubDate']))
    rows = session.execute("SELECT * FROM news_data")
    for row in rows:
        print(row)


    es = Elasticsearch([{'host' : 'elasticsearch', 'port' : 9200, 'scheme' : 'http'}])
    es.indices.create(index = 'news_index', ignore=400)
    
    for story in storyList:
        es.index(index="news_index", body=story)
    resp = es.search(index="news_index", body={"query":{"match_all": {}}})
    print(resp)




