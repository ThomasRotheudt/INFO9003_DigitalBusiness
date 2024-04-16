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
from datetime import datetime
import requests
import json


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

    
def extract_author(soup):
    # Find the <script> tag with the type 'application/ld+json'
    script_tag = soup.find('script', {'type': 'application/ld+json'})

    # Load the JSON content from the script tag
    json_data = json.loads(script_tag.string)

    # Extract the author name
    # Check if 'author' exists and is not empty
    if 'author' in json_data and json_data['author']:
        author_name = json_data['author'][0].get('name', '').strip()
        return author_name
    else:
        author_name = "No author found"
        return author_name
    


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




def get_meta(soup, meta_name):
    """
    Remove unwanted balises and words from the content of the article
    Keep only from the beginning of the article to the end of the article
    """
    
        # Find the <script> tag with the type 'application/ld+json'
    script_tag = soup.find('script', {'type': 'application/ld+json'})

    if not script_tag:
        return None

    # Load the JSON content from the script tag
    json_data = json.loads(script_tag.string)

    # Extract the article body
    article_body = json_data.get(meta_name, '')

    return article_body


    
#----------------------------------------- 
#Crawler functions   
#-----------------------------------------     
        
def getPage(y, month):
    """
    getting index page of the NBC archive
    """
    print("Getting the full page...")
    URL = f"https://www.nbcnews.com/archive/articles/{y}/{month}"
    page = requestLinkWithRetry(URL)    
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup
    
def getArticlesLinks(soup):	
    links = []
    # Find the main element with class 'MonthPage'
    main_content = soup.find('main', class_='MonthPage')
    if main_content:
        a_tags = main_content.find_all('a')
        print("Number of articles found: ", len(a_tags))
        for a in a_tags:
            links.append(a['href'])
    else:
        print("No 'MonthPage' class found in the HTML content.")

    save_soup_to_file(links, "links.html")
    return links[:50] #return the first 50 links for processing reasons. Further improvement could be to get all links

	
	
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

        #Keeping only news and politics articles
        type        = get_meta(a, "articleSection")
        print(type)
        if(type != "news" and type != "politics"):
            continue
       
        content = get_meta(a, "articleBody")   # Find all <p> tags in the article
        if(not content):           # If no <p> tags are found, skip the article
            print("Nothing here")
            continue

        save_soup_to_file(content, f"content_cleaned{i}.html")
        code        = content.rfind("});")
        if(code > 0):            
            content = content[code:]
        editorec    = content.rfind("Editors' Recommendations")
        if(editorec > 0):            
            content = content[0:editorec]
            
        #extract various meta data
        author      = extract_author(a)
        title       = get_meta(a, "headline")
        date        = get_meta(a, "datePublished")
        URL         = get_meta(a, "url")   

        #format date
        date_obj    = datetime.fromisoformat(date.replace('Z', '+00:00'))         # Parse the date using datetime
        date        = date_obj.strftime('%Y/%m/%d')                                   # Format the date as 'YYYY/MM/DD'    
        
        #format data for storage
        metaData    = {"author":author.replace("\n",""),
                        "title":title.replace("\n",""),
                        "type":type.replace("\n",""), 
                        "URL":URL}
        formArt.append({"date":date,"metaData":metaData,"txt":content})
    
    return formArt

#----------------------------------------- 
#Main
#----------------------------------------- 
if __name__ == '__main__':        

    #set crawl time period
    years   = [str(y) for y in range(2024,2025)]
    months  = ["january","february","march","april","may","june","july","august","september","october","november","december"]
    
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
            formArt  = getFormattedArticles(articles, links)
            if(len(formArt) > 0):
                saveToMongo("NBC",formArt)     
                
                