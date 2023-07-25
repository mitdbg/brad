from torch.utils.data import Dataset


class QueryDataset(Dataset):
    def __init__(self, queries, idxs):
        self.queries = queries
        self.idxs = [int(i) for i in idxs]
        assert len(self.queries) == len(self.idxs)

    def __len__(self):
        return len(self.queries)

    def __getitem__(self, i: int):
        return self.idxs[i], self.queries[i]
