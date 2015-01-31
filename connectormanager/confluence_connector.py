#!/usr/bin/python2.4
#
# Based on Google's sitemap_connector from 2010
#
# Author yuri.chemolosov@radialpoint.com (Jan 2015)

import connector
from xmlrpclib import Server

class ConfluenceConnector(connector.TimedConnector):
  """A connector that calls Confluence API to obtain space list, 
     it then iterates all teh spaces to get list of the pages and feed them to GSA
  """

  CONNECTOR_TYPE = 'confluence-connector'
  CONNECTOR_CONFIG = {
      'confluence_host': { 'type': 'text', 'label': 'Confluence host name' },
      'confluence_user': { 'type': 'text', 'label': 'Confluence user' },
      'confluence_pass': { 'type': 'text', 'label': 'Confluence password' },
      'delay': { 'type': 'text', 'label': 'Recrawl delay' }
  }

  def init(self):
    #hardcoded interval for the first run
    self.setInterval(5)

  def run(self):
    #setting interval for the next run
    delay = int(self.getConfigParam('delay'))
    self.setInterval(delay)

    # the parameters into the 'run' method
    self.logger().info('%s connector next run in %s s' % (self.getName(), delay))

    # now go get the sitemap.xml file itself
    host = self.getConfigParam('confluence_host')
    user = self.getConfigParam('confluence_user')
    password = self.getConfigParam('confluence_pass')

    self.logger().debug(' logging %s %s:%s' % (host, user, password))

    self.confluence = Server("http://%s/rpc/xmlrpc" % host)
    self.conf_token = self.confluence.confluence2.login(user, password)
    spaces = self.confluence.confluence2.getSpaces(self.conf_token)

    n = 0
    
    for space in spaces:

      feed_type = 'metadata-and-url'
      #feed_type = 'incremental'
      feed = connector.Feed(feed_type)

      n += self.make_feed(feed, space)

      self.pushFeed(feed)

      feed.clear()

    self.logger().info('Congrats, work done! %d URLs have been posted to GSA.' % n)

  #compose a feed from all the URLs in this space
  def make_feed(self, feed, space):

    i = 0

    self.logger().debug("Reading confluence space %s" % space['name'])
    try:
        pages = self.confluence.confluence2.getPages(self.conf_token, space['key'])
    except:
        self.logger().error("Error: unable to read pages from the space. Skipping.")
        return 0

    for page in pages:
      url = "http://confluence.corp.radialpoint.com/pages/viewpage.action?pageId=%s" % page['id']
      page_title = page['title']

      feed.addRecord(url=url, displayurl=url, action='add', mimetype='text/html', metadata={'content_source': 'Confluence'})

      i += 1

    return i

#================== unit test ===================
if __name__ == '__main__':
  import logging

  debug = True

  config = '''
<ConnectorConfig>
  <ConnectorName>confluence</ConnectorName>
  <ConnectorType>confluence-connector</ConnectorType>
  <Lang>en</Lang>
  <GlobalNamespace>Default</GlobalNamespace>
  <LocalNamespace>Default_confluence</LocalNamespace>
  <Update>false</Update>
  <Param name="confluence_host" value="confluence.corp.radialpoint.com"/>
  <Param name="confluence_pass" value="gsaradialpoint"/>
  <Param name="confluence_user" value="viewer"/>
  <Param name="delay" value="43200"/>
  <ConnectorConfigXml><![CDATA[null]]></ConnectorConfigXml>
</ConnectorConfig>
'''
  # ------ moking up the connector object -------
  logger = logging.getLogger('test')
  ch = logging.StreamHandler()
  formatter = logging.Formatter(
      '[%(asctime)s %(name)s %(levelname)s] %(message)s',
      '%Y/%m/%d:%H:%M:%S')
  ch.setFormatter(formatter)
  logger.addHandler(ch)
  if debug:
    logger.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
  else:
    logger.setLevel(logging.INFO)

  class ManagerMock:
    def __init__(self):
      self.gsa = 'localhost'
      self.log = logger
      self.debug_flag = debug
    
    def logger(self,obj):
      return self.log

  r = ConfluenceConnector(ManagerMock(),'conf-test',config,None,None)

  def pushFeed(feed):
    logger.debug(feed.toXML())

  r.pushFeed = pushFeed
  r.run()