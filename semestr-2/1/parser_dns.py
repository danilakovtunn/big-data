import os
from bs4 import BeautifulSoup
import json

# func parse html file
def parse_eldorado(file):
    with open(file, 'r') as f:
        text = f.read()
    soup = BeautifulSoup(text, features="html.parser")
    entity = {}
    for element in soup.find('div', {'class': 'product-characteristics-content', 'data-role': 'specs'}).find_all('div', class_='product-characteristics__spec'):
        characteristic = [elem.get_text() for elem in element]
        entity[characteristic[0].strip()] = characteristic[1].strip()
    return entity

ans = []
for filename in os.listdir('../0/dns/'):
    ans.append(parse_eldorado('../0/dns/' + filename))

with open('dns_html.json', 'w', encoding='utf-8') as f:
    json.dump(ans, f, ensure_ascii=False)
