from difflib import SequenceMatcher
import pandas as pd


def check_similarity(keywords):
    groups = []
    groups_map = {}
    groups_index = {}

    for keyword in keywords:
        added = False
        for i, group in enumerate(groups):
            if SequenceMatcher(None, keyword, group[0]).ratio() > 0.8:
                group.append(keyword)
                groups_map[keyword] = i
                groups_index[i] += [keyword]
                added = True
                break

        if not added:
            groups.append([keyword])
            groups_map[keyword] = len(groups) - 1
            groups_index[len(groups) - 1] = [keyword]

    return groups_map, groups_index


if __name__ == "__main__":
    print(SequenceMatcher(None, "PA 청약", "전환").ratio())
