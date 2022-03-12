import urllib.request
import xml.etree.ElementTree as ET


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





