https://itunes.apple.com/search?term=radiohead

import sys
import requests
from time import sleep
from confluent_kafka import Producer
from socket import gethostname
reload(sys)
sys.setdefaultencoding('utf8')
conf = {'bootstrap.servers': "sandbox-hdp:6667",'client.id': gethostname()}
kafka_topic = 'itunes_search'

tunes_record = {
    "results": {
    "wrapperType": "audiobook",
    "artistId": 284145822,
    "collectionId": 329945732,
    "artistName": "Alan Cross",
    "collectionName": "Radiohead: The Alan Cross Guide (Unabridged)",
    "collectionCensoredName": "Radiohead: The Alan Cross Guide (Unabridged)",
    "artistViewUrl": "https://books.apple.com/us/artist/alan-cross/284145822?uo=4",
    "collectionViewUrl": "https://books.apple.com/us/audiobook/radiohead-the-alan-cross-guide-unabridged/id329945732?uo=4",
    "artworkUrl60": "https://is4-ssl.mzstatic.com/image/thumb/Features/v4/f5/2b/ae/f52bae83-3dd4-a899-3d41-a86822b4906d/dj.ppzdcauz.jpg/60x60bb.jpg",
    "artworkUrl100": "https://is4-ssl.mzstatic.com/image/thumb/Features/v4/f5/2b/ae/f52bae83-3dd4-a899-3d41-a86822b4906d/dj.ppzdcauz.jpg/100x100bb.jpg",
    "collectionPrice": 3.99,
    "collectionExplicitness": "notExplicit",
    "trackCount": 1,
    "copyright": "© 2009 HarperCollins Publishers Ltd",
    "country": "USA",
    "currency": "USD",
    "releaseDate": "2009-09-01T07:00:00Z",
    "primaryGenreName": "Biographies & Memoirs",
    "previewUrl": "https://audio-ssl.itunes.apple.com/itunes-assets/AudioPreview123/v4/c3/b6/c6/c3b6c657-1898-622b-f6ba-c4ca8640d45a/mzaf_3621485871191436227.std.aac.p.m4a",
    "description": "In this new audiobook, broadcaster and music writer Alan Cross narrates the definitive history of Radiohead. Cross succeeds once again in revealing the fascinating history behind the music in the same compelling way we've come to expect from his long broadcasting career."
}}
records = []
producer = Producer(conf)
for term in tunes_record:
    response = (requests.get("https://itunes.apple.com/search?term={0}".format(tunes_record[term]['artistName'])))
    records.append(response)
    producer.produce(kafka_topic, key="results", value=response.text)


    
    
    
    

    

  
 
 
