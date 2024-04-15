from bs4             import BeautifulSoup
from pymongo         import MongoClient
from multiprocessing import Pool
import progressbar
import datetime
import requests
import time
import sys

#----------------------------------------- 
#Utils functions
#----------------------------------------- 
def saveToMongo(collName, data):  
    clt  = MongoClient('localhost', 27017)
    db   = clt['PhDData']
    coll = db[collName]
    coll.insert_many(data)

def save_soup_to_file(soup, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(str(soup))
    
def getBs4ElementOrEmptyString(soup, tag, values):
    try:
        return soup.findAll(tag, values)[0].get_text()
    except IndexError:
        print("IndexError l 27")
        return ""

def requestLinkWithRetry(link):
    res = None
    tries = 0
    while(tries < 3):
        try:
            res = requests.get(link)
            return res
        except TimeoutError: 
            tries += 1
    return None

def saveSoupsToRepo(soups, repo, name):
    print("Saving soups to repository...")
    for i, soup in enumerate(soups):
        filename = f"{repo}/{name}_{i}.html"
        save_soup_to_file(soup, filename)


def clean_content(tags):
    # Combine text from all <p> tags and add a newline after each
    combined_text = '\n'.join(tag.get_text() for tag in tags if tag)
    
    return combined_text
    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(y, m):
    print("Getting the full page...")
    URL = 'https://www.cnbc.com/site-map/articles/2023/January/1/'
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):
	save_soup_to_file(soup, "soupIndex.html")
	
	links = []
	ul = soup.findAll("a", {"class": 'SiteMapArticleList-link'})   

	#save_soup_to_file(ul, "ul.html")
	print("Nombre d'article trouvé : ", len(ul))
	for a in ul:  # Iterate directly over each 'a' element found
		links.append(a['href'])
	save_soup_to_file(links, "links.html")
	return links
	
def getArticlesFromLinks(links):
    print("Extracting articles from links...")
    #Request all articles in parralel
    p = Pool(10)
    results = p.map(requestLinkWithRetry, links)
    p.close()
    
    #make soup from articles
    soups = []
    
    for result in progressbar.progressbar(results):
        if(not result is None):
            soups.append(BeautifulSoup(result.content, 'html.parser'))
    
    #save individual soup in soup repository
    saveSoupsToRepo(soups, "soups", "soup")
    return soups


    
def getFormattedArticles(articles):
    print("Formatting articles...")
    formArt = []
    for i, a in enumerate(progressbar.progressbar(articles)):
       
        content = a.findAll("p")   #what is this for? 
        
        if(not content):
            print("Nothing here")
            continue
        
        save_soup_to_file(content, f"filtered/content_{i}.html")

        #extract content and clean
        content      = clean_content(content)
        save_soup_to_file(content, f"cleaned/wholeText_{i}.html")
        code         = content.rfind("});")
        if(code > 0):            
            content = content[code:]
        editorec = content.rfind("Editors' Recommendations")
        if(editorec > 0):            
            content = content[0:editorec]
            
        #extract various meta data
        author      = getBs4ElementOrEmptyString(a,"a", {"class": "author"})
        title       = getBs4ElementOrEmptyString(a,"h1", {"class": "b-headline__title"})
        type        = getBs4ElementOrEmptyString(a,"div", {"class": "b-headline__crumbs"})
                
        #extract and format time data
        time        = a.findAll("time", {"class": "b-byline__time"})
        if(len(time)>0):
            time = time[0]
        else:
            continue
        time        = datetime.datetime.strptime(time["datetime"][0:-6], '%Y-%m-%dT%H:%M:%S')
        date        = f'{time.year}-{time.month}-{time.day}'
        
        #format data for storage
        metaData    = {"author":author.replace("\n",""),
                        "title":title.replace("\n",""),
                        "type":type.replace("\n","")}
        formArt.append({"date":date,"metaData":metaData,"txt":content})
    return formArt

#----------------------------------------- 
#Main
#----------------------------------------- 
if __name__ == '__main__':        

    #set crawl time period
    years   = [str(y) for y in range(2023,2024)]
    months  = [str(m) for m in range(12,13)]
    
    #Crawl main loop
    links   = []
    for y in years:
        for m in months:
            print('------------------------')
            print(f'CURRENT PAGE : {y}/{m}')
            print('------------------------')

            page     = getPage(y,m)
            links    = getArticlesLinks(page) 

            if(len(links) == 0):
                print("Nothing here")
                continue
            articles = getArticlesFromLinks(links)
            formArt  = getFormattedArticles(articles)
            if(len(formArt) > 0):
                saveToMongo("CNBC",formArt)     
                
                