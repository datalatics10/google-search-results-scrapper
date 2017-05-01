import sqlite3
import datetime
import hashlib
import os
import urllib2
import urllib

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from DAL import ScrapperDB


query = raw_input("Please enter your query: ")

RESULTS_LOCATOR = "//div/h3[@class='r']/a"

driver = webdriver.PhantomJS()
driver.set_window_size(1120, 550)

query = query.replace(" ", "+")

driver.get("https://www.google.com/search?q="+query)

page1_results = driver.find_elements(By.XPATH, RESULTS_LOCATOR)

count = 0;
targetList=[]

for item in page1_results:
	if item.get_attribute("class") or item.get_attribute("href").encode("utf-8").strip().find("/search?q=")!=-1: continue
	text = item.text.encode("utf-8").strip()
	url = item.get_attribute("href").encode("utf-8").strip()
	# print text, url
	targetList.append({'text': text, 'url' : url})
	count+=1
	if count==3:
		break

Db = ScrapperDB()
Db.OpenConnection()

for i, listItem in enumerate(targetList):

	# print  'Scrapping URL: ' + listItem['url']

	title = listItem['text']
	driver.get(listItem['url'])

	website_url = driver.current_url.encode("utf-8").strip()
	print 'Scrapping Website: \"' + website_url + '\"'

	source = driver.page_source.encode('utf-8').strip()
	plain_text = driver.find_element_by_tag_name("body").text.encode('utf-8').strip()

	dirname = "./"+title.replace(" ","_")+str(i);

	hash_object = hashlib.md5(plain_text)
	phashedText = hash_object.hexdigest()
	rows = Db.URL_select(website_url, phashedText)

	if len(rows) > 0:
		print 'The website \"'+ website_url + '\" is already scrapped since the last change.'
	else:
		print 'Saving results in directory: '+ dirname
		pdirname = dirname
		if not os.path.isdir(dirname): 
			os.mkdir(dirname)

		with open(dirname+"/source.html","w") as f:
			f.write(source)

		with open(dirname+"/plaintext.txt","w") as p: 
			p.write(plain_text)


		all_images = driver.find_elements_by_tag_name("img")

		if not os.path.isdir(dirname+"/images"): 
			os.mkdir(dirname+"/images")

		for image in all_images :
			src = image.get_attribute("src")
			try: 
				if src.lower().endswith(('.png', '.jpg', '.jpeg','.bmp', '.tiff', '.gif','.svg')):
					imagepath = dirname+"/images/" + src.split("/")[len(src.split("/"))-1]
					print imagepath
					urllib.urlretrieve(src, imagepath)
			except:
				print "error at getting image name"

		pmodified = datetime.datetime.now()
		Db.URL_insert(website_url, phashedText, pdirname, pmodified)

Db.CloseConnection()

driver.close()
