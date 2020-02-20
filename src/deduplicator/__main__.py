import collections
import csv
import hashlib
import os
import pathlib


def walk_paths(src):
    return (path for path in src.glob("**/*") if path.is_file())


def every_nth(nth):
    amount = 0
    while True:
        for _ in range(nth - 1):
            yield False
        amount += 1
        yield nth * amount


def stream_progress(stream):
    for index, item in zip(every_nth(10000), stream):
        if index:
            print(index)
        yield item


def read_file_streamed(file_name, chunk_size=2048):
    with open(file_name, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def build_table(paths):
    table = {}
    for path in paths:
        table.setdefault(path.stat().st_size, []).append(path)
    return table


def filter_hash(dict, predicate):
    return {key: value for key, value in dict.items() if predicate(key, value)}


def filter_single_hashes(_, value):
    return len(value) > 1


def stream_hash(iter, hashes):
    for chunk in iter:
        for hash in hashes:
            hash.update(chunk)


def gen_hashes(file_name, *hashes, chunk_size=2048):
    hashes_ = tuple([hashlib.new(hash) for hash in hashes])
    stream_hash(
        read_file_streamed(file_name), hashes_,
    )
    return (hash.hexdigest() for hash in hashes_)


def populate_filter_table(table):
    for size in sorted(table.keys()):
        yield from sorted(
            (size, *gen_hashes(path, "md5", "sha256"), path) for path in table[size]
        )


def populate_file(file_name, items):
    with open(file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for item in items:
            writer.writerow([str(i) for i in item])


def build_index(root, index_name):
    paths = stream_progress(walk_paths(root))
    table = build_table(paths)
    filtered_table = filter_hash(table, filter_single_hashes)
    hashed_paths = stream_progress(populate_filter_table(filtered_table))
    populate_file(index_name, hashed_paths)


def read_csv(file_name):
    with open(file_name, newline="", encoding="utf-8") as f:
        for size, *hashes, path in csv.reader(f):
            yield (int(size), *hashes, pathlib.Path(path))


def table_by_data(hashed_paths):
    by_paths = {}
    by_data = {}
    for hashed_path in hashed_paths:
        by_paths[hashed_path[-1]] = hashed_path
        by_data.setdefault(hashed_path[:-1], []).append(hashed_path)
    return by_paths, by_data


class DupeNode:
    def __init__(self, path, children):
        self.path = path
        self.children = children
        self.duplicates = sum(child.duplicates or child.duplicate for child in children)
        self.total = sum(child.total for child in children) or 1
        self.duplicate = False

    def __repr__(self):
        return (
            f"DupeNode({self.path}, {self.duplicate}, {self.duplicates}, {self.total})"
        )

    @property
    def percentage(self):
        return (self.duplicates or self.duplicate) / self.total

    def get_highest_duplicates(self, threshold=1):
        if self.percentage >= threshold or self.duplicate:
            yield self
        else:
            for child in self.children:
                yield from child.get_highest_duplicates(threshold)

    def __iter__(self):
        yield self
        for child in self.children:
            yield from child


class PathNode:
    def __init__(self, path):
        self.path = path
        self.children = []

    def bind_duplicates(self, by_path, by_data):
        new_children = [
            child.bind_duplicates(by_path, by_data) for child in self.children
        ]
        node = DupeNode(self.path, new_children)
        if self.path in by_path:
            node.duplicate = any(
                not str(path).startswith(DUPLICATE_ROOT)
                for *_, path in by_data[by_path[self.path][:-1]]
            )
        return node


def build_tree(root):
    tree_graph = {root: PathNode(root)}
    for path in stream_progress(root.glob("**/*")):
        tree_graph[path] = self = PathNode(path)
        parent = tree_graph.get(path.parent)
        if parent is not None:
            parent.children.append(self)
    return tree_graph[root]


def deduplicator(root, index):
    by_paths, by_data = table_by_data(stream_progress(read_csv(index)))
    print("building tree")
    tree = build_tree(root)
    print("finding duplicates")
    return tree.bind_duplicates(by_paths, by_data)


def filter_duplicates(duplicates):
    for node in sorted(duplicates, key=lambda n: n.duplicates, reverse=True):
        if node.duplicates <= 20:
            continue
        if not str(node.path).startswith(DUPLICATE_ROOT):
            continue
        yield node
        print(f"{node.total}  {node.path} ({node.percentage:%})")


def main(index):
    index = pathlib.Path(index)
    root = pathlib.Path(input("root: "))
    if not root.exists():
        raise ValueError("Root does not exist")

    to_build_index = not index.exists() or input("rebuild index: ").lower() in (
        "y",
        "yes",
    )
    manual = input("manual: ") in ("n", "no")

    if to_build_index:
        build_index(root, index)

    root = deduplicator(root, index)
    if manual:
        dups = root.get_highest_duplicates(1)
        for node in filter_duplicates(dups):
            print(f"{node.total}  {node.path} ({node.percentage:%})")
    else:
        files = (
            node
            for node in root
            if node.duplicate
            and node.path.is_file()
            and str(node.path).startswith(DUPLICATE_ROOT)
        )
        for node in stream_progress(files):
            node.path.unlink()


DUPLICATE_ROOT = "H:\\_old\\"
if __name__ == "__main__":
    try:
        main("index.csv")
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        raise
        raise SystemExit(1) from None
