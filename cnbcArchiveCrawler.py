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
    db   = clt['CrawlerArticles']
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

def extract_title(soup):
    
    
    # Find the <meta> tag with property='og:title'
    meta_tag = soup.find('meta', property='og:title')
    
    # Extract the content attribute if the meta tag is found
    if meta_tag and meta_tag.has_attr('content'):
        return meta_tag['content']
    else:
        # Return a default or empty string if no title is found
        return "No title found"

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
        
def getPage(y, month, d):
    print("Getting the full page...")
    URL = f'https://www.cnbc.com/site-map/articles/{y}/{month}/{d}/'
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):
	#save_soup_to_file(soup, "soupIndex.html")
	
	links = []
	ul = soup.findAll("a", {"class": 'SiteMapArticleList-link'})   

	#save_soup_to_file(ul, "ul.html")
	print("Nombre d'article trouvé : ", len(ul))
	for a in ul:  # Iterate directly over each 'a' element found
		links.append(a['href'])
	#save_soup_to_file(links, "links.html")
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
    #saveSoupsToRepo(soups, "soups", "soup")
    return soups

def extract_author(soup):
    # Create a BeautifulSoup object
    
    # Find the <meta> tag with name='author'
    meta_tag = soup.find('meta', attrs={'name': 'author'})
    
    # Extract the content attribute if the meta tag is found
    if meta_tag and meta_tag.has_attr('content'):
        return meta_tag['content']
    else:
        # Return a default or empty string if no author is found
        return "No author found"
    
def extract_date(soup):
    # Find the <meta> tag with itemprop='dateCreated'
    meta_tag = soup.find('meta', itemprop='dateCreated')
    
    if meta_tag and meta_tag.has_attr('content'):
        # Parse the date string
        date_string = meta_tag['content']
        # Parse the date using datetime
        date_obj = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
        # Format the date as 'YYYY/MM/DD'
        formatted_date = date_obj.strftime('%Y/%m/%d')
        return formatted_date
    else:
        return "No date found"
    
def getFormattedArticles(articles, links):
    print("Formatting articles...")
    formArt = []
    for i, a in enumerate(progressbar.progressbar(articles)):
       
        content = a.findAll("p")   #what is this for? 
        
        if(not content):
            print("Nothing here")
            continue
        
        #save_soup_to_file(content, f"filtered/content_{i}.html")

        #extract content and clean
        content      = clean_content(content)
        #save_soup_to_file(content, f"cleaned/wholeText_{i}.html")
        code         = content.rfind("});")
        if(code > 0):            
            content = content[code:]
        editorec = content.rfind("Editors' Recommendations")
        if(editorec > 0):            
            content = content[0:editorec]

            
        #extract various meta data
        author      = extract_author(a)
        title       = extract_title(a)
        type        = "CNBC Business Article"
        date        = extract_date(a)
        
        #format data for storage
        metaData    = {"author":author.replace("\n",""),
                        "title":title.replace("\n",""),
                        "type":type.replace("\n",""), 
                        "URL":links[i]}
        formArt.append({"date":date,"metaData":metaData,"txt":content})
    return formArt

#----------------------------------------- 
#Main
#----------------------------------------- 
if __name__ == '__main__':        

    #set crawl time period
    years   = [str(y) for y in range(2023,2024)]
    months  = ["January","February","March","April","May","June","July","August","September","October","November","December"]
    
    #Crawl main loop
    links   = []
    for y in years:
        for m in months[-1:]:
            for d in range(10,13):
                print('------------------------')
                print(f'CURRENT PAGE : {y}/{m}/{d}')
                print('------------------------')

                page     = getPage(y,m,d)
                links    = getArticlesLinks(page) 

                if(len(links) == 0):
                    print("Nothing here")
                    continue
                articles = getArticlesFromLinks(links)
                formArt  = getFormattedArticles(articles, links)
                if(len(formArt) > 0):
                    saveToMongo("CNBC",formArt)     
                
                