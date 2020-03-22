#!/usr/bin/python

# Splunk task processor.
# This processor will execute a series of SPL commands and allow one to
# bulk forecast leveraging the fit command

from urllib.parse import urlencode
import httplib2
import concurrent.futures
from xml.dom import minidom
from bs4 import BeautifulSoup
import logging

# setup
baseurl = 'https://10.121.6.130:8089'
userName = 'admin'
password = 'passw0rd'

# process one is to clear the target
searchquery = '| makeresults count=1  | eval sourcetype="N/A" ' \
              '| eval rever="N/A" ' \
              '| eval test=now() | outputlookup YO6.csv'

# Process two will create the driver ids. Only a single column
# should be returned

searchquery2 = ' | inputlookup test2.csv | dedup sourcetype | head 20'

# Process three will take the above id and replace it where the KEYKEY
# sections are found

searchquery3 = 'index= * | where sourcetype="KEYKEY" ' \
               '| eval rever=sourcetype  | head 2 | eval test=now() ' \
               '| rex field=rever  mode=sed "s/(.)(.)/\\2\\1/" ' \
               '| outputlookup append=true YO6.csv'

LOG_FILENAME = 'splunk_task_processor.log'

# end setup

logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

# this function will run against splunk and return a list of values
# in an XML document


def get_splunk_driver_data(baseurl, userName, password, searchquery):
    serverContent = \
        httplib2.Http(
            disable_ssl_certificate_validation=True
        ).request(baseurl + '/services/auth/login', 'POST',
                  headers={}, body=urlencode({'username':
                                              userName,
                                              'password':
                                              password}
                                             ))[1]
    sessionKey = minidom.parseString(serverContent).getElementsByTagName(
        'sessionKey')[0].childNodes[0].nodeValue
    searchquery = searchquery.strip()
    if not (searchquery.startswith('search') or searchquery.startswith("|")):
        searchquery = 'search ' + searchquery
    logging.info("search query used in thread " + searchquery)
    return_xml = \
        httplib2.Http(
            disable_ssl_certificate_validation=True
        ).request(baseurl +
                  '/services/search/jobs/export',
                  'POST',
                  headers={
                      'Authorization': 'Splunk %s'
                                       % sessionKey},
                  body=urlencode({
                      'search': searchquery}))[1]
    logging.info(return_xml)
    parsed_return_xml = BeautifulSoup(return_xml, 'html.parser')
    return_value = parsed_return_xml.find_all('text')
    return [return_value]


def get_splunk_sql(baseurl, userName, password, searchquery):
    serverContent = \
        httplib2.Http(
            disable_ssl_certificate_validation=True
        ).request(baseurl + '/services/auth/login', 'POST',
                  headers={}, body=urlencode({'username':
                                              userName,
                                              'password':
                                              password}
                                             ))[1]
    sessionKey = \
        minidom.parseString(serverContent).getElementsByTagName(
            'sessionKey')[0].childNodes[0].nodeValue
    searchquery = searchquery.strip()
    if not (searchquery.startswith('search') or searchquery.startswith("|")):
        searchquery = 'search ' + searchquery
    logging.info("search query used in thread " + searchquery)
    x = httplib2.Http(
        disable_ssl_certificate_validation=True).request(
        baseurl + '/services/search/jobs/export',
        'POST',
        headers={'Authorization': 'Splunk %s' % sessionKey},
        body=urlencode({'search': searchquery}))[1]
    logging.info(x)
    z = str(x).lower()
    z = z[0:25]
    if z.find("search not executed") != -1 or z.find("fail") != -1:
        return_value = 1
        print ("ERROR")
        logging.error("ERROR" + searchquery)
        print (z)
    else:
        return_value = 0
    return return_value

logging.info("started Process")

# THIS IS THE PROGRAM START Add tasks here to to prep work
# 1 clear target table
get_splunk_sql(baseurl, userName, password, searchquery)

logging.info("list ran " + searchquery)

# get_splunk_sql(baseurl, userName, password, searchquery2)

# 2 Build list
print("build list " + searchquery2)
logging.info("built master list: " + searchquery2)

driver_list = \
    get_splunk_driver_data(baseurl, userName, password, searchquery2)
control_list_counter = 1
str_list = str(driver_list).split(",")

# Extract control number from XML tags
list_of_control_numbers = []
while control_list_counter < len(str_list):
    control_number = (str((
        str_list[control_list_counter])).replace("<text>", "").
                      replace(
        "</text>", "").replace("[", "").replace("]", "").strip())
    list_of_control_numbers.append(control_number)
    # print("search | index=\"adfadsf\" | where cn=" + control_number)
    searchquerytmp = searchquery3.replace("KEYKEY", control_number)
    # print ("new driver spl " + searchquerytmp )
    # get_splunk_sql(baseurl, userName, password, searchquerytmp)
    control_list_counter = control_list_counter + 1


def fire_splunk_thread(
        replacementkey, searchquery3, baseurl, userName, password):
    searchquery3 = searchquery3.replace("KEYKEY", replacementkey)
    print("Started Thread: " + searchquery3)
    logging.info("Started Thread: " + searchquery3)
    retry = 1
    while retry != 0:
        return_code = get_splunk_sql(
            baseurl, userName, password, searchquery3)
        retry = return_code
    print("Completed: " + searchquery3)
    logging.info("Completed Thread: " + searchquery3)

# We can use a with statement to ensure threads are cleaned up promptly


thread_state = concurrent.futures.ThreadPoolExecutor(max_workers=10)
# Start the load operations and mark each future with its URL
for list_of_control_numbers in list_of_control_numbers:
    print("Started: " + list_of_control_numbers)
    logging.info("Starting the tread " + list_of_control_numbers)
    thread_status = thread_state.submit(
        fire_splunk_thread, list_of_control_numbers,
        searchquery3, baseurl, userName, password)

status = thread_state.shutdown()

print("We completed All Work. Shutting down " + str(status))
logging.info("We completed All Work. Shutting down " + str(status))
