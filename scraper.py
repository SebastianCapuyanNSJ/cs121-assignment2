import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import sys
import os
import tempfile
from typing import List, Dict
from threading import Lock
from collections import Counter

class Token:
    def __init__(self, text: str):
        self.text = text.lower()
    
    def __eq__(self, other):
        if not isinstance(other, Token):
            return False
        return self.text == other.text
    
    def __hash__(self):
        return hash(self.text)
    
    def __str__(self):
        return self.text
    
    def __repr__(self):
        return f"Token('{self.text}')"

def tokenizeText(text: str) -> List[Token]:
    tokens = []
    try:
        for line in text.split('\n'):
            currentToken = []
            
            for char in line:
                try:
                    if (char.isalnum() and char.isascii()):
                        currentToken.append(char)
                    else:
                        if currentToken:
                            tokenStr = ''.join(currentToken)
                            if tokenStr:
                                tokens.append(Token(tokenStr))
                            currentToken = []
                except Exception:
                    if currentToken:
                        tokenStr = ''.join(currentToken)
                        if tokenStr:
                            tokens.append(Token(tokenStr))
                        currentToken = []
                    continue
            if currentToken:
                tokenStr = ''.join(currentToken)
                if tokenStr:
                    tokens.append(Token(tokenStr))
    except Exception as e:
        print(f"Error tokenizing text: {e}", file=sys.stderr)
    
    return tokens

def computeWordFrequencies(tokenList: List[Token]) -> Dict[Token, int]:
    frequencies = {}
    for token in tokenList:
        if token in frequencies:
            frequencies[token] += 1
        else:
            frequencies[token] = 1
    return frequencies

stats = {
    "uniquePages": 0,
    "longestPageUrl": "",
    "longestPageCount": 0,
    "wordFrequencies": {},
    "subdomains": {}
}

statsLock = Lock()

seen_lock = Lock()
seen_urls = set()

def check_if_seen(url: str) -> bool:
    current_url = urldefrag(url)[0]
    with seen_lock:
        if current_url in seen_urls:
            return True
        seen_urls.add(current_url)
        return False

stopWords = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", 
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", 
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", 
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", 
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", 
    "yourselves"
])

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        return []
    if not resp.raw_response or not resp.raw_response.content:
        return []
    
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    except Exception:
        return []
    
    pageUrl = urldefrag(getattr(resp, "url", url) or url)[0]
    expand = True

    try:
        expand = updateStatistics(pageUrl, soup)

    except Exception as e:
        print(f"Error updating stats for {pageUrl}: {e}")
    
    if not expand:
        return []

    extractedLinks = set()

    for link in soup.find_all('a'):
        href = link.get('href')
        if not href:
            continue
        hrefNoFrag = urldefrag(href)[0]
        fullUrl = urljoin(pageUrl, hrefNoFrag)
        extractedLinks.add(fullUrl)    

    return list(extractedLinks)

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        allowedDomains = [
            ".ics.uci.edu",
            ".cs.uci.edu",
            ".informatics.uci.edu",
            ".stat.uci.edu"
        ]
        netloc = parsed.hostname.lower() if parsed.hostname else ""
        if not any(netloc.endswith(d) or netloc == d[1:] for d in allowedDomains):
            return False
        
        lower_path = parsed.path.lower()
        lower_query = parsed.query.lower()

        if "login" in lower_path or "signup" in lower_path or "signin" in lower_path:
            return False
        
        if "auth" in lower_path or "sso" in lower_path:
            return False
        
        if "search" in lower_path or "ical" in lower_path or "events/" in lower_path:
            return False
        
        if "sort=" in lower_query:
            return False
        
        if "outlook" in lower_query or "ical" in lower_query:
            return False

        pathParts = [p for p in parsed.path.split('/') if p]
        if len(pathParts) > 2:
            counts = Counter(pathParts)
            if any(count >= 3 for count in counts.values()):
                return False
            
        if parsed.query:
            loginTraps = {'action', 'login', 'auth', 'sso', 'redirect', 'id', 'token'}
            query_params_check = parsed.query.split('&')
            for param in query_params_check:
                key_check = param.split('=')[0].lower()
                if key_check in loginTraps:
                    return False
                
            trapKeys = {'tribe-bar-date', 'ical', 'tribe_event_display', 
                    'date', 'calendar', 'eventdate'}
        
            query_params = parsed.query.split('&')
            for param in query_params:
                key = param.split('=')[0].lower()
                if key in trapKeys:
                    return False
            
            query_params = parsed.query.split('&')
            if len(query_params) > 3:
                return False
            key_counts = Counter([param.split('=')[0] for param in query_params])
            if any(count > 1 for count in key_counts.values()):
                 return False

        if len(url) > 300:
            return False

        if re.search(r'\d{4}/\d{2}/\d{2}', parsed.path):
            if len(pathParts) > 5:
                return False

        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def updateStatistics(url, soup):
    if check_if_seen(url):
        return False

    parsed = urlparse(url)
    subdomain = None
    if "uci.edu" in parsed.netloc:
        subdomain = parsed.netloc.lower()
    
    textContent = soup.get_text()
    tokens = tokenizeText(textContent)

    validTokens = [t for t in tokens if t.text not in stopWords and len(t.text) > 1]
    tokenCount = len(validTokens)
    newFrequencies = computeWordFrequencies(validTokens)

    with statsLock:
        stats["uniquePages"] += 1

        if subdomain:
            stats["subdomains"][subdomain] = stats["subdomains"].get(subdomain, 0) + 1

        if tokenCount > stats["longestPageCount"]:
            stats["longestPageCount"] = tokenCount
            stats["longestPageUrl"] = url

        for tokenObj, count in newFrequencies.items():
            word = tokenObj.text
            stats["wordFrequencies"][word] = stats["wordFrequencies"].get(word, 0) + count

        if stats["uniquePages"] % 25 == 0:
            dumpReport()
    
    return True

def dumpReport():
    try:
        with open("crawler_report.txt", "w", encoding="utf-8") as f:
            f.write(f"Total Unique Pages Found: {stats['uniquePages']}\n")
            f.write(f"Longest Page: {stats['longestPageUrl']} ({stats['longestPageCount']} words)\n\n")
            
            f.write("Top 50 Common Words:\n")
            sortedWords = sorted(stats["wordFrequencies"].items(), key=lambda x: (-x[1], x[0]))[:50]
            for word, count in sortedWords:
                f.write(f"{word}: {count}\n")
                
            f.write("\nSubdomains found:\n")
            for sub, count in sorted(stats["subdomains"].items()):
                f.write(f"{sub}, {count}\n")
    except Exception as e:
        print(f"Error writing report: {e}")