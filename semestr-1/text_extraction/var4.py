from mrjob.job import MRJob
from mrjob.job import MRStep

resource_str = "Natural resources:  "

class TextExtraction(MRJob):

    def mapper_init(self):
        self.resources = []
        self.was_res = False
        self.closed = True
        
    def mapper(self, _, line):
        if resource_str in line:
            self.was_res = True
            line = line[line.find(resource_str) + len(resource_str):]
            self.resources = line.split(',')

            for i in range(len(self.resources)):
                self.resources[i] = self.resources[i].strip()
            if self.resources[-1] == "":
                self.resources = self.resources[:-1]
            else:
                self.closed = False
        elif self.was_res:
            if line != "":
                resources = line.split(',')
                for i in range(len(resources)):
                    resources[i] = resources[i].strip()
                if not self.closed:
                    self.resources[-1] += " " + resources[0]
                    resources = resources[1:]
                    self.closed = True
                if len(resources) != 0 and resources[-1] == "":
                    resources = resources[:-1]
                else:
                    self.closed = False
                self.resources += resources
            else:
                for resource in self.resources:
                    if "(" in resource:
                        resource = resource.split(" (")[0]
                    yield resource, 1
                self.resources = []
                self.was_res = False
                self.closed = True
            
    def reducer(self, key, value):
        yield key, sum(value)

    def steps(self):
        return ([
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper, reducer=self.reducer),
        ])
    
if __name__ == '__main__':
    TextExtraction.run()