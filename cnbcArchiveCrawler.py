"""
INFO9003  : Advanced Topics in Digital Business 
Project 2 : News Article Crawler
Author    : Maxime Deravet, Thomas Rotheudt, Robin Fombonne
Date      : 2024-04-17
"""

from bs4             import BeautifulSoup
from pymongo         import MongoClient
from multiprocessing import Pool
import progressbar
import datetime
import requests


#----------------------------------------- 
#Utils functions
#----------------------------------------- 
def saveToMongo(collName, data):  
    """
    Save the data in the collection collName of the MongoDB database CrawlerArticles
    collName : str : name of the collection to save the data
    data     : list: list of dictionaries to save in the collection
    """
    clt  = MongoClient('localhost', 27017)
    db   = clt['CrawlerArticles']
    coll = db[collName]
    coll.insert_many(data)

def save_soup_to_file(soup, filename):
    """
    save the soup in a file
    """
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(str(soup))

def saveSoupsToRepo(soups, repo, name):
    """
    Debugging function to save the list of soup in one existing repository
    """
    print("Saving soups to repository...")
    for i, soup in enumerate(soups):
        filename = f"{repo}/{name}_{i}.html"
        save_soup_to_file(soup, filename)
    
def getBs4ElementOrEmptyString(soup, tag, values):
    try:
        return soup.findAll(tag, values)[0].get_text()
    except IndexError:
        print("IndexError l 27")
        return ""

def extract_title(soup):
    """
    extract the title of the article for CNBC source
    """
    meta_tag = soup.find('meta', property='og:title')           # Find the <meta> tag with property='og:title'
    
    if meta_tag and meta_tag.has_attr('content'):               # Extract the content attribute if the meta tag is found
        return meta_tag['content']
    else:
        return "No title found"
    
def extract_author(soup):
    meta_tag = soup.find('meta', attrs={'name': 'author'})      # Find the <meta> tag with name='author'
    
    if meta_tag and meta_tag.has_attr('content'):               # Extract the content attribute if the meta tag is found
        return meta_tag['content']
    else:
        return "No author found"
    
def extract_date(soup):
    
    meta_tag = soup.find('meta', itemprop='dateCreated')        # Find the <meta> tag with itemprop='dateCreated'
    
    if meta_tag and meta_tag.has_attr('content'):
        date_string = meta_tag['content'] 
        date_obj = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')  # Parse the date using datetime
        formatted_date = date_obj.strftime('%Y/%m/%d')                             # Format the date as 'YYYY/MM/DD'    
        return formatted_date
    else:
        return "No date found"

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

def clean_content(tags):
    """
    Remove unwanted balises and words from the content of the article
    Keep only from the beginning of the article to the end of the article
    """
    start_collecting = False                        # Flag to start collecting text 
    collected_texts = []                            # List to store collected text
    
    for tag in tags:
        
        if tag.attrs.get('id') == 'MainContent':    # Check if we have reached the "MainContent" marker
            start_collecting = True
            continue                                # Skip the "MainContent" tag itself
        
        # If we are past the "MainContent" tag, collect the text
        if start_collecting:
            text = tag.get_text(strip=True)
            if text:                                # Ensure not adding empty strings
                collected_texts.append(text)
    
    # Combine all collected texts with a newline
    combined_text = '\n'.join(collected_texts)
    
    return combined_text

    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(y, month, d):
    """
    getting index page of the CNBC archive
    """
    print("Getting the full page...")
    URL = f'https://www.cnbc.com/site-map/articles/{y}/{month}/{d}/'
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):	
	links = []
	ul = soup.findAll("a", {"class": 'SiteMapArticleList-link'})   

	print("Number of articles found : ", len(ul))
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
    
    return soups


    
def getFormattedArticles(articles, links):  #?(change from original is to have links added in metadata)
    print("Formatting articles...")
    formArt = []
    for i, a in enumerate(progressbar.progressbar(articles)):
       
        content = a.findAll("p")   # Find all <p> tags in the article
        if(not content):           # If no <p> tags are found, skip the article
            print("Nothing here")
            continue
        
        #check if article is reserved for paid subscribers, if yes skip
        pro_link = a.find('a', {'class': 'ProPill-proPillLink', 'data-type': 'pro-button'}) #common line in all paid articles
        if pro_link:
            print("Article reserved for paid subscribers. Skipping...")
            continue
        

        #extract content and clean
        content      = clean_content(content)  # Clean the content and keep only the wanted core text
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
            for d in range(10,11):
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
                
                