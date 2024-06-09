import json
import os
import re
from mrjob.job import MRJob

class Dns_mapper(MRJob):
    def mapper(self, i, entity):
        entity = json.loads(entity[:-1])
        ans = {}
        display = {}
        size = {}
        interface = {}
        added = {}
        
        ans['Модель'] = entity['Модель']
        ans['Страна производитель'] = entity['Страна-производитель']
        if 'Гарантия продавца / производителя' not in entity:
            garanty_str = entity['Гарантия продавца']
        else:
            garanty_str = entity['Гарантия продавца / производителя']
        ans['Гарантия'] = int(re.findall(r'\d+', garanty_str)[0])
        ans['Операционная система'] = entity['Операционная система']

        display['Диагональ экрана'] = int(re.findall(r'\d+', entity['Диагональ экрана (дюйм)'])[0])
        display['Разрешение экрана'] = re.findall(r'\d+x\d+', entity['Разрешение экрана'])[0]
        display['Тип матрицы'] = re.findall(r'(.*)-', entity['Тип'])[0]
        ans['Экран'] = display

        size['Высота'] = entity['Высота без подставки']
        size['Ширина'] = entity['Ширина без подставки']
        size['Глубина'] = entity['Толщина без подставки']
        if 'Вес без подставки' in entity:
            weight = entity['Вес без подставки']
        else:
            weight = entity['Вес без подставки, измерено в ДНС']
        size['Вес'] = weight
        ans['Габариты'] = size

        interface['Количество HDMI'] = int(entity['Количество HDMI портов'])
        interface['Количество USB'] = int(re.findall(r'\d+', entity['Количество USB'])[0])
        hasWIFI = False
        if entity['Wi-Fi'] == 'встроенный':
            hasWIFI = True
        interface['Наличие Wi-Fi'] = hasWIFI
        ans['Интерфейс'] = interface

        isSmart = False
        if entity['Поддержка Smart TV'] == 'есть':
            isSmart = True
        added['Smart TV'] = isSmart
        ans['Дополнительно'] = added
        yield i, ans

class Eldorado_mapper(MRJob):
    def mapper(self, i, entity):
        entity = json.loads(entity[:-1])
        ans = {}
        display = {}
        size = {}
        interface = {}
        added = {}
        
        ans['Модель'] = entity['Модель']
        ans['Страна производитель'] = entity['Страна']
        ans['Гарантия'] = int(int(re.findall(r'\d+', entity['Гарантия'])[0])) * 12
        if 'Операционная система' not in entity:
            ans['Операционная система'] = 'Нет'
        else:
            ans['Операционная система'] = entity['Операционная система']

        display['Диагональ экрана'] = int(re.findall(r'(\d+)"', entity['Диагональ'])[0])
        display['Разрешение экрана'] = re.findall(r'\d+x\d+', entity['Разрешение экрана'])[0]
        display['Тип матрицы'] = entity['Технология']
        ans['Экран'] = display

        size['Высота'] = entity['Высота']
        size['Ширина'] = entity['Ширина']
        size['Глубина'] = entity['Глубина']
        if 'Вес без подставки' in entity:
            size['Вес'] = entity['Вес без подставки']
        else:
            size['Вес'] = entity['Вес']
        ans['Габариты'] = size

        interface['Количество HDMI'] = int(entity['HDMI'])
        interface['Количество USB'] = int(re.findall(r'\d+', entity['Количество разъемов USB'])[0])
        hasWIFI = False
        if entity['Wi-Fi'] == 'Есть':
            hasWIFI = True
        interface['Наличие Wi-Fi'] = hasWIFI
        ans['Интерфейс'] = interface

        isSmart = False
        if entity['Smart TV'] == 'Есть':
            isSmart = True
        added['Smart TV'] = isSmart
        ans['Дополнительно'] = added
        yield i, ans

def main():
    dns_data = []
    with open("dns_html.json") as f:
        text = json.loads(f.read())

    with open("tmp.json", "w", encoding="utf8") as f:
        for item in text:
            json.dump(item, f, ensure_ascii=False)
            f.write(",\n")
    
    mapper = Dns_mapper(["tmp.json"])
    with mapper.make_runner() as runner:
        runner.run()
        for _, attributes in mapper.parse_output(runner.cat_output()):
            dns_data.append(attributes)

    with open('dns_final.json', "w", encoding="utf8") as f:
        json.dump(dns_data, f, ensure_ascii=False, indent=4)
    

    eldorado_data = []
    with open("eldorado_html.json") as f:
        text = json.loads(f.read())

    with open("tmp.json", "w", encoding="utf8") as f:
        for item in text:
            json.dump(item, f, ensure_ascii=False)
            f.write(",\n")
    
    mapper = Eldorado_mapper(["tmp.json"])
    with mapper.make_runner() as runner:
        runner.run()
        for _, attributes in mapper.parse_output(runner.cat_output()):
            eldorado_data.append(attributes)

    with open('eldorado_final.json', "w", encoding="utf8") as f:
        json.dump(eldorado_data, f, ensure_ascii=False, indent=4)

    os.remove("tmp.json")

if __name__ == '__main__':
    main()