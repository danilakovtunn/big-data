from mrjob.job import MRJob
from mrjob.step import MRStep

class VertexCount(MRJob):

    def mapper1(self, _, line):
        src, dst, _ = line.split()
        yield (src, None)
        yield (dst, None)
    
    def reducer1(self, vertex, _):
        yield (vertex, 1)
    
    def mapper2(self, vertex, count):
        yield (None, count)
    
    def reducer2(self, _, counts):
        yield ('total_count', sum(counts))
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2),
        ]



if __name__ == '__main__':
    VertexCount().run()
