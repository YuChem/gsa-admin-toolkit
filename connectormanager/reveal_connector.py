#!/usr/bin/python2.4
#
# Based on Google's sitemap_connector from 2010
#
# Author yuri.chemolosov@radialpoint.com (Jan 2015)

import connector
import hashlib, json, os, re
from os.path import isfile, isdir, join
from xmlrpclib import Server
from xml.sax.saxutils import escape

JSON_LOCATION = "C:/Users/yuric/Projects/Reveal/2014-12-12"

class RevealConnector(connector.TimedConnector):
  """A connector that parses Reveal content (set of JSON files as it was crawled by 80legs)
     and feeds each file to GSA
  """

  CONNECTOR_TYPE = 'reveal-connector'
  CONNECTOR_CONFIG = {'delay': { 'type': 'text', 'label': 'Recrawl delay'}}
  RECORD_PER_FEED = 10  

  def init(self):
    #hardcoded interval for the first run
    self.setInterval(5)

  def run(self):
    #setting interval for the next run
    delay = int(self.getConfigParam('delay'))
    self.setInterval(delay)

  # the parameters into the 'run' method
    self.logger().info('%s connector next run in %s s' % (self.getName(), delay))

    self.logger().debug('reading files from %s' % JSON_LOCATION)

    json_files = [ join(JSON_LOCATION,f) for f in os.listdir(JSON_LOCATION) if isfile(join(JSON_LOCATION,f)) and re.match('.*\.json$', f) ]

    self.logger().debug('  total %d files' % len(json_files))

    #clear GSA feed first
    self.logger().debug('sending empty ''full'' feed to reset GSA')
    feed = connector.Feed("full")
    self.pushFeed(feed)
    feed.clear()

    n = 0

    feed = connector.Feed('incremental')

    for fname in json_files:
      with open(fname, 'r') as f:
        pages = json.loads(f.read())
        self.logger().info("%s: has %d records" % (fname, len(pages)))

        per_feed_counter = 0

        #slice record array into smaller slis
        for page in pages:

          url,html = (page['url'], page.get('result',None))

          if(html):
            url_hash = hashlib.md5(url).hexdigest() 
            feed.addRecord(url="http://reveal/recommendation/%s" % url_hash, 
                           displayurl=escape(url), 
                           action='add', 
                           mimetype='text/html', 
                           metadata={'content_source': 'Reveal',
                                     'rp_elastic_id': url_hash},
                           content=html)
            
            per_feed_counter += 1
            if per_feed_counter < self.RECORD_PER_FEED:
              continue

          self.pushFeed(feed)
          feed.clear()
          n += per_feed_counter
          per_feed_counter = 0
        else:
          self.pushFeed(feed)
          feed.clear()
          n += per_feed_counter

      self.logger().info('Congrats, work done! %d pages have been posted to GSA.' % n)

#================== unit test ===================
if __name__ == '__main__':
  config = '''
<ConnectorConfig>
  <ConnectorName>reveal_crawl</ConnectorName>
  <ConnectorType>reveal-connector</ConnectorType>
  <Lang>en</Lang>
  <GlobalNamespace>Default</GlobalNamespace>
  <LocalNamespace>Default_reveal_crawl</LocalNamespace>
  <Update>false</Update>
  <Param name="delay" value="43200"/>
  <ConnectorConfigXml><![CDATA[null]]></ConnectorConfigXml>
</ConnectorConfig>
'''
  r = RevealConnector(None,None,config,None,None)

