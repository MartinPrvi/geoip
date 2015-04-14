import cPickle
import shelve

#==================================================================================================

def update_database():
  
  
  print '\x1b[1mStart pre-processing\x1b[0m '.ljust(67,'=')
  
  #----------------------------------------------
  print '\x1b[1mImporting modules\x1b[0m'.ljust(60),
  sys.stdout.flush()
  
  import requests
  import zipfile
  import StringIO
  import csv

  
  print '[\x1b[32;1m OK \x1b[0m]'

  #----------------------------------------------
  
  print '\x1b[1mDownloading new GeoIP database\x1b[0m'.ljust(60),
  sys.stdout.flush()
  
  contents = open('GeoIP/GeoLite2-City-CSV.zip','r').read()
  contents = None
  if not contents:
    r  = requests.get('http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip')
  
    if r.status_code != requests.codes.ok:
      print '[\x1b[31;1mFAIL\x1b[0m]'
      print '\t---',r.status_code
      print '\n\x1b[1maborting\x1b[0m'
      return
    contents = r.content
    print '[\x1b[32;1m OK \x1b[0m]'
  
  #----------------------------------------------
  
  print '\x1b[1mExtracting files\x1b[0m'.ljust(60),
  sys.stdout.flush()
  contents   = zipfile.ZipFile(StringIO.StringIO(contents))
  testzip    = contents.testzip()
  
  if testzip:
    print '[\x1b[31;1mFAIL\x1b[0m]'
    print '\t---', testzip, 'is corrupted'
    print '\n\x1b[1maborting\x1b[0m'
    return
  print '[\x1b[32;1m OK \x1b[0m]'
  
  #----------------------------------------------
  
  files   = contents.namelist()
  version = files[0].split('_')[1].split('/')[0]
  dirname = files[0].split('/')[0]
  
  print '\t\x1b[1mVersion:\x1b[0m', version
  print
  #----------------------------------------------

  
  print '\x1b[1mParsing Cities\x1b[0m'.ljust(60),
  sys.stdout.flush()
  
  try:
    with contents.open(dirname+'/'+'GeoLite2-City-Locations-en.csv', 'r') as f:
      cities       = {}
      csv_cities   = csv.reader(f)

      next(csv_cities)

      for city in csv_cities:
        

        cities[city[0]]=city[1:]  #{geoname_id : [locale_code, continent_code, continent_name, country_iso_code, country_name, subdivision_1_iso_code, subdivision_1_name, subdivision_2_iso_code, subdivision_2_name, city_name, metro_code, time_zone, *postal_code, *latitude, *longitude]}
        
     
    
        
  except Exception as e:
    print '[\x1b[31;1mFAIL\x1b[0m]'
    print '\t---', str(e)
    print '\n\x1b[1maborting\x1b[0m'
    return
  print '[\x1b[32;1m OK \x1b[0m]'
  
  print '\t\x1b[1mNumber of cities:\x1b[0m', str(len(cities)).rjust(5)
  print
  #----------------------------------------------
  

  
  print '\x1b[1mParsing IPv4\x1b[0m'.ljust(60),
  sys.stdout.flush()
  
  try:
    with contents.open(dirname+'/'+'GeoLite2-City-Blocks-IPv4.csv', 'r') as f:
      networks = {}
      
      csv_networks   = csv.reader(f)

      next(csv_networks)
      
      for network in csv_networks:
        if network[1] == '':
          if network[2] == '':
            continue
          geoname_id = network[2]
        else:
          geoname_id = network[1]
        
        if (INCLUDE_COUNTRIES and cities.get(geoname_id,['','','',''])[3] in INCLUDE_COUNTRIES) or not INCLUDE_COUNTRIES:
          networks[network[0]] = network[1:]       # {network : [geoname_id, registered_country_geoname_id, represented_country_geoname_id, is_anonymous_proxy, is_satellite_provider, postal_code, latitude, longitude]}
          if len(cities.get(network[1],['','','','','','','','','','','','','','','',]))>12:
            pass
          else:
            cities[network[1]].extend(network[5:])

    serialize(cities,'GeoIP/cities')
  except Exception as e:
    print '[\x1b[31;1mFAIL\x1b[0m]'
    print '\t---', str(e)
    print '\n\x1b[1maborting\x1b[0m'
    return
  print '[\x1b[32;1m OK \x1b[0m]'
  print '\t\x1b[1mCountries included:\x1b[0m',
  if INCLUDE_COUNTRIES:
    print str(len(INCLUDE_COUNTRIES)).rjust(6)
  else:
    print 'All'
  print '\t\x1b[1mNumber of networks:\x1b[0m', str(len(networks)).rjust(6)
  print
  
  del cities

  
  #----------------------------------------------  
  
  print '\x1b[1mGenerating binary tree\x1b[0m'.ljust(60),
  sys.stdout.flush()
  try:
    t = generate_binary_tree(networks)
    del networks
    WEB_tree = t[0]
    del t[0]
    
  except Exception as e:
    print '[\x1b[31;1mFAIL\x1b[0m]'
    print '\t---', str(e)
    print '\n\x1b[1maborting\x1b[0m'
    return
  print '[\x1b[32;1m OK \x1b[0m]'
  print '\t\x1b[1mMinimum depth:     \x1b[0m', str(t[0]).rjust(6)
  print '\t\x1b[1mMaximum depth:     \x1b[0m', str(t[1]).rjust(6)
  print '\t\x1b[1mAverage depth:     \x1b[0m', str(t[2]).rjust(6)
  print
  #----------------------------------------------
  
  print '\x1b[1mSaving binary tree\x1b[0m'.ljust(60),
  sys.stdout.flush()
  try:
    size = serialize(WEB_tree,'GeoIP/WEB_tree_cities')
    del WEB_tree

    
  except Exception as e:
    print '[\x1b[31;1mFAIL\x1b[0m]'
    print '\t---', str(e)
    print '\n\x1b[1maborting\x1b[0m'
    return
  print '[\x1b[32;1m OK \x1b[0m]'
  print '\t\x1b[1mSize:\x1b[0m', ("%0.1f" % (size/1024.0)).rjust(6),'KB'
  #----------------------------------------------
  
  print
  print '\x1b[1mFinished pre-processing\x1b[0m '.ljust(67,'=')

  
#==================================================================================================
def tree():
  return defaultdict(tree)
#==================================================================================================

def generate_binary_tree(networks):

  WEB_tree  = defaultdict(tree)
  
  min_depth = 32
  max_depth = 0
  avg_depth = 0
  
  
  for network, info in networks.iteritems():
    network     = ip_to_binary(network,True)
    add_to_tree(WEB_tree,  network, info[0])
  
    network_len  =  len(network)
    avg_depth   +=  network_len
    if network_len < min_depth:
      min_depth = network_len
    if network_len > max_depth:
      max_depth = network_len
  
  
  
  avg_depth/=len(networks)
  import json
  return [convert_u(json.loads(json.dumps(WEB_tree))), min_depth, max_depth, avg_depth]
  
#==================================================================================================

def add_to_tree(t, path, leave):
  for node in path[:-1]:
    t = t[node]
  t[path[-1]] = leave
  
#==================================================================================================

def ip_to_binary(ip,with_subnet=False):
  if not with_subnet:
    return ''.join([bin(int(x)+256)[3:] for x in ip.split('.')]).rjust(32,'0')
    
  network = ip.split('/')
  subnet  = int(network[1])
  ip      = network[0]
  
  return ''.join([bin(int(x)+256)[3:] for x in ip.split('.')]).rjust(32,'0')[:subnet]

#==================================================================================================

def binary_to_ip(bin_ip):
  subnet = len(bin_ip)
  bin_ip = bin_ip.ljust(32,'0')
  
  
  ip=[]
  
  for octet in [bin_ip[0+i:8+i] for i in range(0, subnet, 8)]:
    ip.append(str(int(octet,2)))
  ip='.'.join(ip)
  
  return ip,subnet
  
#==================================================================================================

def serialize(obj,filename,pickle=False):
  
  import os
  
  if not pickle:
    filename+='.shelve'
    sh = shelve.open(filename, protocol=2, writeback=True)
    for k,v in obj.iteritems():
      sh[k]=v
    sh.close()
  
  else:
    filename+='.pickle'
    cPickle.dump(obj, open(filename,'w'), 2)

  return os.path.getsize(filename)
  
#==================================================================================================

def deserialize(filename,pickle=False):
  if not pickle:
    return shelve.open(filename+'.shelve', flag = 'r', protocol=2, writeback = True)
  else:
    return cPickle.load(open(filename+'.pickle','r'))
  
#==================================================================================================

def ip_to_binary(ip,with_subnet=False):
  if not with_subnet:
    return ''.join([bin(int(x)+256)[3:] for x in ip.split('.')]).rjust(32,'0')
    
  network = ip.split('/')
  subnet  = int(network[1])
  ip      = network[0]
  
  return ''.join([bin(int(x)+256)[3:] for x in ip.split('.')]).rjust(32,'0')[:subnet]
  
#==================================================================================================

def convert_u(inp):
  if isinstance(inp, dict):
    return {convert_u(key): convert_u(value) for key, value in inp.iteritems()}
  elif isinstance(inp, list):
    return [convert_u(element) for element in inp]
  elif isinstance(inp, unicode):
    return inp.encode('utf-8')
  else:
    return inp
    
#==================================================================================================

def get_ip_info(ip,open_tree=False):

  if open_tree:
    current = deserialize('GeoIP/WEB_tree_cities')
  else:
    current = WEB_tree
    
    
    
  for bit in ip_to_binary(ip):
    
    current = current.get(bit)

    if not current:
      return None

    if isinstance(current, str):
      return current
    
#==================================================================================================

def is_mk_ip(ip):
  return bool(get_ip_info(ip) == 'MK')

#==================================================================================================

def test():
  
  import time
  import random
  
  print
  print '\x1b[1mTesting...\x1b[0m'.ljust(60)
  print
  
  print '\x1b[1m1. Known IP addresses:\x1b[0m'
  print
  
  for ip in ['77.28.184.30','77.29.197.4','91.185.212.194','92.53.38.80','54.93.60.204','50.87.150.205']:
    info      = get_ip_info(ip)
    print '\x1b[1mIP:\x1b[0m', ip,'\t',
    print '\x1b[1mLocation:\x1b[0m',
    if not info:
      print 'Unknown'
    else:
      c_info = CITIES_INFO.get(info)
      print c_info[6], c_info[9], c_info[4], c_info[3], c_info[14:]
      
      
  
  
  print
  print '\x1b[1m2. 100.000 randomly generated IP addresses, with pickle already loaded:\x1b[0m',
  sys.stdout.flush()
  ips = []
  for x in xrange(100000):
    ips.append(".".join(map(str, (random.randint(0, 255) 
                        for _ in range(4)))))
  
  
  t1 = time.time()
  for ip in ips:
    get_ip_info(ip)
  t2 = time.time()
  print t2-t1,'seconds'
  print
  
  
  '''
  iters = 1000
  if not INCLUDE_COUNTRIES:
    iters = 10
  
  print '\x1b[1m3.', iters, 'randomly generated IP addresses, load pickle on every check:   \x1b[0m',
  sys.stdout.flush()

  
  t1 = time.time()
  for ip in ips[:iters]:
    get_ip_info(ip,True)
  t2 = time.time()
  print t2-t1,'seconds'
  print
  '''
  
  
  
  print 'bye.'
  
#==================================================================================================




if __name__ == '__main__':

  import sys
  from collections import defaultdict

  
  
  
  
  INCLUDE_COUNTRIES = set(['MK'])
  INCLUDE_COUNTRIES = set([])
  
  
  if len(sys.argv) >= 2:
    if str(sys.argv[1]).lower() != "update_db":
      WEB_tree = deserialize('GeoIP/WEB_tree_cities')
      CITIES_INFO = deserialize('GeoIP/cities')
    if len(sys.argv) == 2:
      
      if str(sys.argv[1]).lower() == "update_db":
        import os
        if not os.path.exists('GeoIP'):
          os.makedirs('GeoIP')
        
        update_database()
    
    
      #--------------------------------------------
    
      elif str(sys.argv[1]).lower() == "ip_info":
        while True:
          try:
            info      = get_ip_info(raw_input('IP: '))

            print '\x1b[1mLocation:\x1b[0m',
            if not info:
              print 'Unknown'
            else:
              c_info = CITIES_INFO.get(info)
              print info
              print c_info[6], c_info[9], c_info[4], c_info[3], c_info[14:]
          except Exception as e:
            print str(e)
          
      #--------------------------------------------
      
      elif str(sys.argv[1]).lower() == "test":
        test()
    #--------------------------------------------------------
  
  
    elif len(sys.argv) == 3:

      if str(sys.argv[1]).lower() == "convert_ip":
        print ip_to_binary(sys.argv[2])
    
      #--------------------------------------------
    
      elif str(sys.argv[1]).lower() == "convert_network":
        print ip_to_binary(sys.argv[2],True)
  
      #--------------------------------------------
  
      elif str(sys.argv[1]).lower() == "is_mk":
        print is_mk_ip(sys.argv[2])
    #====================================================
  
    #get_ip_info('91.185.212.194')
  else:
    #update_database()
    CITIES_INFO = deserialize('GeoIP/cities')
    WEB_tree = deserialize('GeoIP/WEB_tree_cities')
    test()
    
else:
  WEB_tree = deserialize('../GeoIP/WEB_tree_cities')
  CITIES_INFO = deserialize('../GeoIP/cities')
