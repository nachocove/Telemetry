from token import *


class Cluster:
    def __init__(self):
        self.max_wildcards = 10
        self.num_wildcard = 0
        self.samples = list()
        self.pattern = None

    def __str__(self):
        return '%d: %s' % (len(self.samples), self.pattern)

    def __len__(self):
        return len(self.samples)

    def add(self, token_list):
        if self.pattern is None:
            # First sample. Just accept it
            self.pattern = token_list
            self.samples.append(token_list)
            half_len = len(self.pattern) / 2
            if self.max_wildcards > half_len:
                self.max_wildcards = half_len
            return True

        # Match against existing pattern.
        pattern_len = len(self.pattern)
        if pattern_len != len(token_list):
            return False

        # No match see how much adjustment we need to do
        delta = self.pattern - token_list
        if (self.num_wildcard + delta) > self.max_wildcards:
            return False  # too much difference

        # Adjust pattern
        for n in range(pattern_len):
            if self.pattern.tokens[n] != token_list.tokens[n]:
                self.pattern.tokens[n] = Token()
                self.num_wildcard += 1

        # Save the sample
        self.samples.append(token_list)
        assert self.num_wildcard <= self.max_wildcards
        return True


class Clusterer:
    def __init__(self):
        self.clusters = []

    def _add_cluster(self, token_list):
        new_cluster = Cluster()
        added = new_cluster.add(token_list)
        assert added
        self.clusters.append(new_cluster)

    def add(self, token_list):
        if len(self.clusters) == 0:
            self._add_cluster(token_list)
            return

        # We have existing clusters. See if any of them matches
        for cluster in self.clusters:
            if cluster.add(token_list):
                break
        else:
            # Nothing matches. Add a new cluster
            self._add_cluster(token_list)