from mrjob.job import MRJob
from mrjob.job import MRStep

org_str = "International organization participation:  "

class TextExtraction(MRJob):

    def mapper_init(self):
        self.orgs = []
        self.was_org = False
        self.closed = True
        
    def mapper(self, _, line):
        if org_str in line:
            self.was_org = True
            line = line[line.find(org_str) + len(org_str):]
            self.orgs = line.split(',')

            for i in range(len(self.orgs)):
                self.orgs[i] = self.orgs[i].strip()
            if self.orgs[-1] == "":
                self.orgs = self.orgs[:-1]
            else:
                self.closed = False
        elif self.was_org:
            if line != "":
                orgs = line.split(',')
                for i in range(len(orgs)):
                    orgs[i] = orgs[i].strip()
                if not self.closed:
                    self.orgs[-1] += " " + orgs[0]
                    orgs = orgs[1:]
                    self.closed = True
                if len(orgs) != 0 and orgs[-1] == "":
                    orgs = orgs[:-1]
                else:
                    self.closed = False
                self.orgs += orgs
            else:
                for org in self.orgs:
                    if "(" in org:
                        org = org.split(" (")[0]
                    if "note" in org:
                        org = org.split(" note")[0]
                    if org[-1].isdigit():
                        org = ''.join(org.split())
                    yield org, 1
                self.orgs = []
                self.was_org = False
                self.closed = True
            
    def reducer(self, key, value):
        yield None, (key, sum(value))

    def reduce_sort(self, key, value):
        for key, count in sorted(value, reverse=True, key=lambda x: (x[1], x[0])):
            yield key, count

    def steps(self):
        return ([
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reduce_sort)
        ])
    
if __name__ == '__main__':
    TextExtraction.run()