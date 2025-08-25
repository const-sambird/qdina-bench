import glob
import os
import re

def load_test_set_queries(path: str) -> tuple[list[str], list[int]]:
    '''
    Load the pregenerated queries in the training set located at `path`.
    Returns the query text in a list as well as which template each query belongs to.
    '''
    # first: how many templates are there, and how many queries do each one of them have?
    all_queries = glob.glob(f'{path}/*.sql')
    query_names = [os.path.basename(q) for q in all_queries]
    name_parts = [q.split('_') for q in query_names]
    template_strs = list(set([t[0] for t in name_parts]))
    query_nums = list(set([int(q[1].strip('.sql')) for q in name_parts]))

    queries = []
    templates = []

    for i, template in enumerate(template_strs):
        for query_num in query_nums[i]:
            with open(f'{path}/{template}_{query_num}.sql', 'r') as infile:
                lines = infile.readlines()
            query = ' '.join(lines[1:])
            query = query.replace('\n', ' ').replace('\t', ' ')
            query = update_query_text(query)
            queries.append(query)
            templates.append(int(template) - 1)
    
    return queries, templates

def update_query_text(text: str) -> str:
    '''
    Updates query text to work in PostgreSQL.

    Taken from https://github.com/hyrise/index_selection_evaluation

    :param text: the text of the query to update
    :returns text: the corrected version
    '''
    text = text.replace(";\nlimit ", " limit ").replace("limit -1", "")
    text = re.sub(r" ([0-9]+) days\)", r" interval '\1 days')", text)
    text = add_alias_subquery(text)
    return text

# PostgreSQL requires an alias for subqueries
def add_alias_subquery(query_text):
    text = query_text.lower()
    positions = []
    for match in re.finditer(r"((from)|,)[  \n]*\(", text):
        counter = 1
        pos = match.span()[1]
        while counter > 0:
            char = text[pos]
            if char == "(":
                counter += 1
            elif char == ")":
                counter -= 1
            pos += 1
        next_word = query_text[pos:].lstrip().split(" ")[0].split("\n")[0]
        if next_word[0] in [")", ","] or next_word in [
            "limit",
            "group",
            "order",
            "where",
        ]:
            positions.append(pos)
    for pos in sorted(positions, reverse=True):
        query_text = query_text[:pos] + " as alias123 " + query_text[pos:]
    return query_text