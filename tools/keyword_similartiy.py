from difflib import SequenceMatcher
import pandas as pd

def check_similarity(keywords):
    groups = []
    groups_map = {}

    for keyword in keywords:
        added = False
        for i, group in enumerate(groups):
            if SequenceMatcher(None, keyword, group[0]).ratio() > 0.8:
                group.append(keyword)
                groups_map[keyword] = i
                added = True
                break

        if not added:
            groups.append([keyword])
            groups_map[keyword] = len(groups) - 1
    
    return groups, groups_map


if __name__ == '__main__':
    print(SequenceMatcher(None, "PA 청약", "전환").ratio())