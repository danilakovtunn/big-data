import os
import json
from mrjob.job import MRJob


class Duplicate_Finder(MRJob):
    def mapper(self, _, line):
        ''' 
        block key are
         - Страна-производитель
         - Экран/Диагональ экрана
        '''
        entity = json.loads(line[:-1])
        yield f"{entity['Страна производитель']}, {entity['Экран']['Диагональ экрана']}", entity

    def reducer(self, block_key, entities):
        lst = list(entities)
        duplicates = []
        for entity in lst:
            for duplicate_group in duplicates:
                if self.isDuplicate(entity, duplicate_group):
                    duplicate_group.append(entity)
                    break
            else:
                duplicates.append([entity])

        yield (block_key, duplicates)

    def isDuplicate(self, entity, duplicate_group, threshold=0.84):
        for duple in duplicate_group:
            countTrue = 0
            total = 0
            for key, value in entity.items():
                match value:
                    case int() | str() | bool():
                        countTrue += value == duple[key]     
                        total += 1                   
                    case float():
                        countTrue += abs(value - duple[key]) < 2
                        total += 1
                    case dict():
                        for k, v in value.items():
                            match v:
                                case int() | str() | bool():
                                    countTrue += v == duple[key][k]
                                    total += 1
                                case float():
                                    countTrue += abs(v - duple[key][k]) < 2
                                    total += 1
            if countTrue / total < threshold:
                return False
        return True



def main():
    data = []
    
    with open("../1/dns_final.json") as f:
        data.extend(json.load(f))
    with open("../1/eldorado_final.json") as f:
        data.extend(json.load(f))

    with open("tmp.json", "w", encoding="utf8") as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write(",\n")

    ans = []
    duplicate_finder = Duplicate_Finder(["tmp.json"])
    with duplicate_finder.make_runner() as runner:
        runner.run()
        for _, duplicate_list in duplicate_finder.parse_output(runner.cat_output()):
            for duplicates in duplicate_list:
                ans.append({"duplicates": duplicates})

    with open("duplicates.json", "w", encoding="utf8") as f:
        json.dump(ans, f, ensure_ascii=False, indent=4)

    os.remove("tmp.json")

if __name__ == '__main__':
    main()