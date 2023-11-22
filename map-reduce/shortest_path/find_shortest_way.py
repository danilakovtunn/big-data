from mrjob.job import MRJob
from mrjob.step import MRStep

class HaveNegativeCycle(Exception):
    pass

class Find_shortest_way(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('-s', '--startnode', default='1', type=str, help='starting node for shortest away')
        self.add_passthru_arg('-t', '--total_count', type=int, help='total count of vertexes')

    def initial_mapper(self, _, line):
        src, dest, weight = line.split()
        if src == self.options.startnode:
            value = 0
        else:
            value = 'inf'
        yield src, (value, [src, dest, int(weight)])
    
    def initial_reducer(self, key, values):
        lst = []
        for i in values:
            lst.append(i[1])
            value = i[0]    
        yield key, (value, lst)
    
    def mapper(self, key, value):
        distance = value[0]
        yield key, value
        if distance != 'inf':
            for i in value[1]:
                yield i[1], (distance + i[2], [])

    def reducer(self, key, values):
        minimum = 'inf'
        lst = []
        for i in values:
            curr_dist = i[0]
            if minimum == 'inf':
                minimum = curr_dist
            elif curr_dist != 'inf':
                minimum = min(minimum, curr_dist)
            if len(i[1]) != 0:
                lst = i[1]

        yield key, (minimum, lst)

    def final_reducer(self, key, values):
        minimum = 'inf'
        last_min = 'no values'
        for i in values:
            curr_dist = i[0]
            if minimum == 'inf':
                minimum = curr_dist
            elif curr_dist != 'inf':
                minimum = min(minimum, curr_dist)
            if len(i[1]) != 0:
                last_min = curr_dist

        if minimum != 'inf' and last_min != 'no values' and last_min > minimum:
            # there is cycle with negative value
            raise HaveNegativeCycle

        yield key, minimum


    def steps(self):
        return [
            MRStep(
                mapper=self.initial_mapper,
                reducer=self.initial_reducer,
            ),
        ] + [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            )
        ] * (self.options.total_count - 1) + [
            MRStep(
                mapper=self.mapper,
                reducer=self.final_reducer,
            ),
        ]
    
    
if __name__ == '__main__':
    try:
        Find_shortest_way.run()
    except HaveNegativeCycle:
        print('graph have a cycle with negative value')