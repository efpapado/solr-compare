import pysolr
import sys
import csv
import settings


def fetch_results(collection):
    params = {
        'rows': 200000,
        'sort': 'id ASC',
        # 'start': 0,
        'cursorMark': '*',
    }
    docs = []
    total = False
    while True:
        _result = collection.search('*', **params)

        if not total:
            total = _result.hits

        _docs = _result.docs

        perc = (len(docs) + len(_docs)) / total * 100
        print('Fetching {} {:.5f}%'.format(collection.url, perc))

        docs.extend(_docs)

        if params['cursorMark'] == _result.nextCursorMark:
            break

        else:
            params['cursorMark'] = _result.nextCursorMark
            continue

    return _docs


def compare_results(_res_before, _res_after):
    count_before = len(_res_before)
    count_after = len(_res_after)

    if count_after != count_before:
        sys.exit("Different number of results.")

    _diff_docs = []
    _diff_lite = {}
    for i in range(count_before):
        doc_before = _res_before[i]
        doc_after = _res_after[i]
        keys = list(doc_before.keys())
        keys.remove('_version_')
        has_diff = False
        for key in keys:
            value_before = doc_before[key]
            value_after = doc_after[key]
            if isinstance(value_before, list):
                value_before.sort()
            if isinstance(value_after, list):
                value_after.sort()

            if value_before != value_after:
                has_diff = True

                if doc_before['id'] not in _diff_lite:
                    _diff_lite[doc_before['id']] = {}
                if key not in _diff_lite[doc_before['id']]:
                    _diff_lite[doc_before['id']][key] = {}

                _diff_lite[doc_before['id']][key] = {
                    'before': value_before,
                    'after': value_after,
                }

        if has_diff:
            _diff_docs.append({
                'before': doc_before,
                'after': doc_after,
            })
    return _diff_docs, _diff_lite


def write_report(_diff_values):
    csv_columns = ['id', 'field', 'before', 'after']
    csv_file = "diff.csv"
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=';', fieldnames=csv_columns)
            writer.writeheader()
            for _id in _diff_values:
                _item = _diff_values[_id]
                for _field in _item:
                    _values = _item[_field]
                    writer.writerow({
                        'id': _id,
                        'field': _field,
                        'before': _values['before'],
                        'after': _values['after'],
                    })
    except IOError:
        print("I/O error")


# Script starts here

before = pysolr.Solr(settings.SOLR_BEFORE, timeout=120, always_commit=True)
res_before = fetch_results(before)

after = pysolr.Solr(settings.SOLR_AFTER, timeout=120, always_commit=True)
res_after = fetch_results(after)

diff_raw = compare_results(res_before, res_after)
diff_docs = diff_raw[0]
diff_values = diff_raw[1]

if len(diff_values):
    write_report(diff_values)
