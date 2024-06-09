import os
import json
import random
from mrjob.job import MRJob


class Merger(MRJob):
    def mapper(self, key, line):
        entity = json.loads(line[:-1])
        yield key, entity

    def reducer(self, key, duplicates):
        for duplicate in duplicates:
            entities = duplicate['duplicates']
            if len(entities) == 1:
                yield key, entities[0]
            else:
                new_entity = {}
                for key in entities[0].keys():
                    if type(entities[0][key]) == dict:
                        for k in entities[0][key].keys():
                            lst = []
                            for entity in entities:
                                lst.append(entity[key][k])
                            if key not in new_entity:
                                new_entity[key] = {}
                            new_entity[key][k] = self.getResolveFuncByKey(k)(lst)
                    else:
                        lst = []
                        for entity in entities:
                            lst.append(entity[key])
                        new_entity[key] = self.getResolveFuncByKey(key)(lst)
                yield (key, new_entity)

    def getResolveFuncByKey(self, key):
        match key:
            case 'Модель':
                return lambda lst: min(lst, key=len)
            case 'Страна производитель':
                return lambda lst: min(lst, key=len)
            case 'Гарантия':
                return lambda lst: min(lst)
            case 'Операционная система':
                return lambda lst: min(lst, key=len)
            case 'Диагональ экрана':
                return lambda lst: sum(lst) / len(lst)
            case 'Разрешение экрана':
                return lambda lst: min(lst, key=len)
            case 'Тип матрицы':
                return lambda lst: min(lst, key=len)
            case 'Высота':
                return lambda lst: sum(lst) / len(lst)
            case 'Ширина':
                return lambda lst: sum(lst) / len(lst)
            case 'Глубина':
                return lambda lst: sum(lst) / len(lst)
            case 'Вес':
                return lambda lst: round(sum(lst) / len(lst))
            case 'Количество HDMI':
                return lambda lst: min(lst)
            case 'Количество USB':
                return lambda lst: min(lst)
            case 'Наличие Wi-Fi':
                return lambda lst: min(lst)
            case 'Smart TV':
                return lambda lst: min(lst)
            case _:
                return lambda lst: random.choice(lst)

def main():    
    with open("../2/duplicates.json") as f:
        data = json.load(f)

    with open("tmp.json", "w", encoding="utf8") as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write(",\n")

    ans = []
    merger = Merger(["tmp.json"])
    with merger.make_runner() as runner:
        runner.run()
        for _, merged in merger.parse_output(runner.cat_output()):
            ans.append(merged)

    with open("merged.json", "w", encoding="utf8") as f:
        json.dump(ans, f, ensure_ascii=False, indent=4)

    os.remove("tmp.json")

if __name__ == '__main__':
    main()