import os
from bs4 import BeautifulSoup
import json

# func parse html file
def parse_eldorado(file):
    with open(file, 'r') as f:
        text = f.read()
    soup = BeautifulSoup(text, features="html.parser")
    entity = {}
    model = soup.find('h1', {'data-dy': 'heading', 'class': 'lg ng Vs go'}).text
    model = model.removeprefix('Характеристики ')
    entity['Модель'] = model
    for element in soup.find(id='TabCharacteristics').find('div', class_='JG').find_all('div', class_='LG'):
        characteristic = [elem.get_text() for elem in element]
        entity[characteristic[0].strip()] = characteristic[1].strip()
    return entity

ans = []
for filename in os.listdir('../0/eldorado/'):
    ans.append(parse_eldorado('../0/eldorado/' + filename))

with open('eldorado_html.json', 'w', encoding='utf-8') as f:
    json.dump(ans, f, ensure_ascii=False)
