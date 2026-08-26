"""Harvest the LUX activity stream and maintain the object-sensor mapping.

Walks the stream backwards from the newest page until it reaches the last
harvest watermark, fetches each changed object, and extracts the sensors from
its Environmental Monitoring activity. Updates an accumulated state table with
valid-from / valid-to dates.

State lives in DATA_DIR so it survives container recreation. It cannot be
rebuilt by re-walking: YUAG keeps stream pages for only 30 days, so once a page
rolls off those events are gone. Treat the state file as primary data.

The watermark only advances on a clean full pass. A partial run leaves it alone
so the next run re-covers the same ground rather than skipping events.
"""
import json, os, sys, time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError

COLLECTION = 'https://media.art.yale.edu/discovery/lux/collection.json'
DATA_DIR   = os.environ.get('DATA_DIR', '/src/data')
STATE      = os.path.join(DATA_DIR, 'object_sensor_state.json')
CUSTOMER   = '307'
FAR_FUTURE = '9999-12-31'

AAT_MONITORING = 'http://vocab.getty.edu/aat/300379380'
AAT_ACCESSION  = 'http://vocab.getty.edu/aat/300312355'
AAT_SYSTEM_NO  = 'http://vocab.getty.edu/aat/300435704'

MAX_PAGES = int(os.environ.get('LUX_MAX_PAGES', '50'))


def fetch(url, tries=3):
    for i in range(tries):
        try:
            with urlopen(url, timeout=30) as r:
                return json.load(r)
        except HTTPError as e:
            if e.code == 404:
                return None
            if i == tries - 1:
                raise
            time.sleep(2 ** i)
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2 ** i)


def as_list(x):
    """LUX returns a bare dict when there is one item, a list when several."""
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return []


def extract(rec):
    """Return (sensor_ids, accession, system_no) or (None, ...) if not monitored."""
    sids = []
    for act in as_list(rec.get('used_for')):
        if not isinstance(act, dict):
            continue
        codes = [c.get('id') for c in as_list(act.get('classified_as')) if isinstance(c, dict)]
        if AAT_MONITORING not in codes:
            continue
        for obj in as_list(act.get('used_specific_object')):
            if not isinstance(obj, dict):
                continue
            for ident in as_list(obj.get('identified_by')):
                if isinstance(ident, dict) and ident.get('content'):
                    sids.append(ident['content'])
                    break          # one identifier per sensor
    if not sids:
        return None, None, None

    acc = sysno = None
    for ident in as_list(rec.get('identified_by')):
        if not isinstance(ident, dict) or ident.get('type') != 'Identifier':
            continue
        codes = [c.get('id') for c in as_list(ident.get('classified_as')) if isinstance(c, dict)]
        if AAT_ACCESSION in codes:
            acc = ident.get('content')
        elif AAT_SYSTEM_NO in codes:
            sysno = ident.get('content')
    return sids, acc, sysno


def load_state():
    if os.path.exists(STATE):
        with open(STATE) as f:
            return json.load(f)
    return {'watermark': None, 'objects': {}}


def save_state(state):
    tmp = STATE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE)


def main():
    state = load_state()
    watermark = state.get('watermark')
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    started = datetime.now(timezone.utc).isoformat()
    print(f"watermark: {watermark or '(none - full walk)'}")

    coll = fetch(COLLECTION)
    page = (coll.get('last') or {}).get('id')

    seen, pages, events, complete = set(), 0, 0, False
    newest_seen = None

    while page and pages < MAX_PAGES:
        d = fetch(page)
        if d is None:
            break
        items = d.get('orderedItems', [])
        items.reverse()                       # newest first
        pages += 1
        stop = False
        for it in items:
            ts = it.get('endTime')
            if ts and newest_seen is None:
                newest_seen = ts
            if watermark and ts and ts <= watermark:
                stop = True
                complete = True
                break
            events += 1
            uri = (it.get('object') or {}).get('id', '')
            if '/obj/' not in uri or uri in seen:
                continue
            seen.add(uri)
            rec = fetch(uri, tries=2)
            if rec is None:
                continue
            sids, acc, sysno = extract(rec)
            key = uri.rsplit('/', 1)[-1].replace('.json', '')
            prev = state['objects'].get(key)
            if sids:
                devices = sorted(f'conserv:{CUSTOMER}:{s}' for s in sids)
                if not prev or prev.get('devices') != devices:
                    state['objects'][key] = {
                        'object_id': uri,
                        'tms': sysno or key,
                        'accession': acc,
                        'label': rec.get('_label'),
                        'devices': devices,
                        'valid_from': today,
                        'valid_to': FAR_FUTURE,
                    }
                    print(f"  changed {key}: {devices}", flush=True)
            elif prev and prev.get('valid_to') == FAR_FUTURE:
                prev['valid_to'] = today
                print(f"  ended   {key}", flush=True)
        if stop:
            break
        nxt = (d.get('prev') or {}).get('id')
        if nxt == page:                        # self-referential guard
            break
        page = nxt
        if page is None:
            complete = True

    if complete and newest_seen:
        state['watermark'] = newest_seen
        state['last_run'] = started
        save_state(state)
        print(f"\ncomplete. watermark advanced to {newest_seen}")
    else:
        save_state(state)
        print(f"\nINCOMPLETE after {pages} pages - state saved, watermark NOT advanced")

    print(f"pages={pages} events={events} objects_fetched={len(seen)} "
          f"tracked={len(state['objects'])}")


if __name__ == '__main__':
    main()
