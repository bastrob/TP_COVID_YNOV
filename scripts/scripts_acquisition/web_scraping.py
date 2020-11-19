# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 10:48:34 2020

@author: Bast
"""

# Install:
# python -m pip install beautifulsoup4 requests html5lib

from bs4 import BeautifulSoup
import requests

url = ("https://raw.githubusercontent.com/bastrob/DataScience_from_Scratch/master/python/simple_html.html")

html = requests.get(url).text
soup = BeautifulSoup(html, "html5lib")


# Retrieve first parapagh
first_paragraph = soup.find('p')

print(first_paragraph)

# Extract the whole text of first paragraph
first_paragraph_text = soup.p.text
print(first_paragraph_text)

# Split words into list
first_paragraph_words = soup.p.text.split()
print(first_paragraph_words)

# Extract a tag attribute
first_paragraph_id = soup.p["id"] # Raises KeyError if no 'id' found
first_paragraph_id2 = soup.p.get("id") # Returns None if no 'id' found

print(first_paragraph_id)

print("**********")
second_paragraph = [p.text for p in soup("p") if p.get("id") == "subject"]
second_paragraph_t = soup.find(id="subject").text
print(second_paragraph)
print(second_paragraph_t)

all_paragraph = soup.find_all('p') # or soup("p")
paragraph_with_ids = [p for p in soup("p") if p.get("id")]

print(paragraph_with_ids)


# Find tags with a specific class
important_paragraphs = soup("p", {'class' : 'important'})
important_paragraphs2 = soup('p', 'important')
important_paragraphs3 = [p for p in soup('p')
                         if 'important' in p.get('class', [])]

print(important_paragraphs)

#Combine methods
# Example: Find every <span> element contained inside a <div> element
spans_inside_divs = [span 
                     for div in soup("div")
                     for span in div("span")]


#Doc: https://www.crummy.com/software/BeautifulSoup/bs4/doc/


# Exercise: find all the representatives who have press releases about data
url = ("https://www.house.gov/representatives")
html = requests.get(url).text
soup = BeautifulSoup(html, "html5lib")

# View in source code to get the links of website
all_urls = [a["href"]
            for a in soup("a") if a.has_attr('href')]

print(len(all_urls)) #Size: 966

# We need to extract only URLs starting by http or https and end with .house.gov or .house.gov/
#print(all_urls)


# Good place to apply regular expression

# https://docs.python.org/3/library/re.html
import re

# Must start with http:// or https://
# Must end with .house.gov or .house.gov/
# ^: matches the start of the string
# ?: https? matches http or https
# .*: matches any url 
# \: special content to match
# \.gov/?: special content to match + either gov or gov/
# $: specifies the end of the match, every url with suppl http://barky.house.gov/XXxx will not be matched

#regex = r"^https?://.*\.house\.gov/?$"
regex = r"^http(s)?://.*\.house\.gov/?$"
#regex_test = r"^http://"

# Test code
assert re.match(regex, "http://barky.house.gov")
assert re.match(regex, "http://barky.house.gov/")
assert re.match(regex, "https://barky.house.gov")
assert re.match(regex, "https://barky.house.gov/")

assert not re.match(regex, "http://house.gov.barky")
assert not re.match(regex, "/sitemap")
assert not re.match(regex, "http://barky.data.com")
assert not re.match(regex, "https://barky.data")
assert not re.match(regex, "http://barky.house.gov/data")
assert not re.match(regex, "https://barky.house.gov/data")


good_urls = [url for url in all_urls if re.match(regex, url)]

print(len(good_urls)) # Size: 872
#print(good_urls)

good_urls = list(set(good_urls))
print(len(good_urls))

print(good_urls)

# Retrieve press release of specific house
jayapal_house_url = ("https://stivers.house.gov/")
html = requests.get(jayapal_house_url).text
soup = BeautifulSoup(html, "html5lib")

links = [a["href"] for a in soup("a") if "press release" in a.text.lower()]

# Better use a set to avoid duplicates
links = {a["href"] for a in soup("a") if "press release" in a.text.lower()}

# Notice that this is a relative link, which means we need to remember the originating site
print(links)

from typing import Dict, Set

press_releases: Dict[str, Set[str]] = {}
for house_url in good_urls:
    html = requests.get(house_url).text
    soup = BeautifulSoup(html, 'html5lib')
    pr_links = {a['href'] for a in soup('a') if 'press releases' in a.text.lower()}
    print(f"{house_url}: {pr_links}")
    press_releases[house_url] = pr_links


# We need to find which congresspeople have press release mentioning data


def paragraph_mentions(text: str, keyword: str) -> bool:
    """
    Returns True if a <p> inside the text mentions {keyword}

    Parameters
    ----------
    text : str
        DESCRIPTION.
    keyword : str
        DESCRIPTION.

    Returns
    -------
    bool
        DESCRIPTION.

    """
    
    soup = BeautifulSoup(text, "html5lib")
    paragraphs = [p.get_text() for p in soup("p")]
    
    return any(keyword.lower() in paragraph.lower() for paragraph in paragraphs)

text = """<body><h1>Facebook</h1><p>Twitter</p></body>"""
assert paragraph_mentions(text, "twitter")
assert not paragraph_mentions(text, "facebook")

for house_url, pr_links in press_releases.items():
    for pr_link in pr_links:
        url = f"{house_url}/{pr_link}"
        text = requests.get(url).text
        
        if paragraph_mentions(text, "data"):
            print(f"{house_url}")
            break

url = ("https://gomez.house.gov/news/documentquery.aspx?DocumentTypeID=27")
text = requests.get(url).text
soup = BeautifulSoup(text, "html5lib")

paragraphs = [p.get_text() for p in soup('p')]


url = ("https://jayapal.house.gov/media/press-releases/")
text = requests.get(url).text
soup = BeautifulSoup(text, "html5lib")

paragraphs = [p.text for p in soup('blockquote')]

for paragraph in paragraphs:
    if "data" in paragraph.lower():
        print(True)
        break
