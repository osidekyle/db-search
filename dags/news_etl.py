import urllib.request
import json
import xml.etree.ElementTree as ET
from cassandra.cluster import Cluster

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

    import http.client
    #conn = http.client.HTTPConnection('localhost:8983')
    conn = http.client.HTTPConnection('solr:8983')
    conn.request('GET', '/solr/admin/cores?action=STATUS')
    res = conn.getresponse()
    data = json.loads(res.read().decode('utf-8'))
    print(data)
    if 'news_core' not in data['status'].keys():
         print("No news_core!")

get_news_data()



