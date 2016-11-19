from twisted.internet import defer
import datetime
from decimal import Decimal

from poolsrv import config
from poolsrv import dbsink

from poolsrv.posix_time import posix_secs

import poolsrv.logger
log = poolsrv.logger.get_logger()


dbsink = dbsink.DbSink(config.PRIMARY_DB)


PUSH_TEMPLATES = {
    'sh_values': "(%(worker_id)d, %(block_id)d, %(shares)d, %(score)f, '%(last_share)s', 1)",
    'sh_body': 'INSERT INTO mining_share ' +
               '(worker_id, block_id, shares, score, last_share, stratum) ' +
               'VALUES %s ' +
               'ON DUPLICATE KEY UPDATE shares=shares+VALUES(shares), ' +
               'score=score+VALUES(score), last_share=greatest(last_share,VALUES(last_share));',
    'sh_values_slots': '''(%(worker_id)d, %(block_id)d, %(slot_num)d, %(slot_score)f, %(shares)d, '%(last_share)s')''',
    'sh_body_slots': '''
        insert into mining_slot_hot (worker_id, block_id, slot_num, slot_score, shares, last_share)
        values %s
        on duplicate key update
            slot_score = slot_score + values(slot_score),
            shares = shares + values(shares),
            last_share = greatest(last_share, values(last_share));''',
    'stats_body': '''INSERT INTO mining_stats
            (name, date, value, slot_num, slot_score, delay, block_id, first_push, last_push, push_count)
            VALUES ('%(name)s', '%(time_window)s', %(shares)s, %(slot_num)d, %(slot_score)f,
                    (unix_timestamp() - %(push_secs)d) div %(delay_size)d * %(delay_size)d,
                    %(block_id)d, from_unixtime(%(push_secs)d), from_unixtime(%(push_secs)d), 1)
            ON DUPLICATE KEY UPDATE value = value + values(value),
            slot_score = slot_score + values(slot_score),
            push_count = push_count + 1,
            first_push = least(first_push, values(first_push)),
            last_push = greatest(last_push, values(last_push));
            '''
}

PUSH_TEMPLATES_NEW = PUSH_TEMPLATES.copy()


def set_push_templates(sh_values, sh_body, sh_values_slots, sh_body_slots, stats_body):
    if sh_values:
        PUSH_TEMPLATES_NEW['sh_values'] = sh_values
    if sh_body:
        PUSH_TEMPLATES_NEW['sh_body'] = sh_body
    if sh_values_slots:
        PUSH_TEMPLATES_NEW['sh_values_slots'] = sh_values_slots
    if sh_body_slots:
        PUSH_TEMPLATES_NEW['sh_body_slots'] = sh_body_slots
    if stats_body:
        PUSH_TEMPLATES_NEW['stats_body'] = stats_body


def switch_push_templates():
    def _forkey(key):
        val = PUSH_TEMPLATES[key]
        PUSH_TEMPLATES[key] = PUSH_TEMPLATES_NEW[key]
        PUSH_TEMPLATES_NEW[key] = val

    _forkey('sh_values')
    _forkey('sh_body')
    _forkey('sh_values_slots')
    _forkey('sh_body_slots')
    _forkey('stats_body')


def get_push_templates():
    return {
        'current': PUSH_TEMPLATES.copy(),
        'new': PUSH_TEMPLATES_NEW.copy()
    }


def submit_share_stats(time_window, push_secs, shares, slot_num, slot_score, block_id):
    stmt = PUSH_TEMPLATES['stats_body'] % \
        {'name': config.STRATUM_UNIQUE_ID,
         'time_window': time_window,
         'shares': shares,
         'block_id': block_id,
         'push_secs': push_secs,
         'slot_num': slot_num,
         'slot_score': slot_score,
         'delay_size': config.STATS_PUSH_DELAY_GROUP_SIZE}

    dbsink.execute('submit_share_stats', stmt)


def submit_shares(shares_iterator):
    """Batch submit"""

    if config.PUSH_SHARES__MINING_SHARES and config.PUSH_SHARES__MINING_SLOTS:
        # We need to materialize possible generator because we need to process
        # the data twice
        shares_iterator = list(shares_iterator)

    if config.PUSH_SHARES__MINING_SHARES or not config.PUSH_SHARES__MINING_SLOTS:
        # This is unsafe way to build SQL, but we know all these
        # values are coming from internal source
        records = (PUSH_TEMPLATES['sh_values'] % worker_shares
                   for worker_shares in shares_iterator)
        stmt = PUSH_TEMPLATES['sh_body'] % (', \n'.join(records))

        dbsink.execute('submit_shares', stmt)

    if config.PUSH_SHARES__MINING_SLOTS:
        records = (PUSH_TEMPLATES['sh_values_slots'] % worker_shares
                   for worker_shares in shares_iterator)
        stmt = PUSH_TEMPLATES['sh_body_slots'] % (', \n'.join(records))

        dbsink.execute('submit_shares_slots', stmt)


def _create_new_block_orig(date_started=None):
    # Let's create new one!
    log.info("Creating new mining_block record")

    if not date_started:
        date_started = datetime.datetime.now()

    stmt = '''INSERT INTO mining_block
           (founder, date_started, date_found, total_shares, blocknum, is_mature)
            VALUES
           ('', '%(date_started)s', '%(null_date)s', 0, 0, -2)''' % \
           {'null_date': config.NULL_DATE, 'date_started': date_started}

    dbsink.execute('_create_new_block_orig', stmt)


def submit_block_orig(block_id, worker_name, date_found, block_hash, block_height, difficulty, value):

    # Creates a new template for the following (not yet found) block.
    _create_new_block_orig(date_found)

    # Fill-in block information and make it complete.
    stmt = '''UPDATE mining_block SET
            is_mature = -2,
            founder = '%(worker_name)s',
            date_found = '%(date_found)s',
            hash = '%(hash)s',
            difficulty = %(difficulty)s,
            value = %(value)s,
            workers = 0,
            blocknum = %(block_height)s
            WHERE id = %(block_id)s''' % \
            {'worker_name': worker_name,
             'date_found': date_found,
             'hash': block_hash,
             'difficulty': difficulty,
             'block_height': block_height,
             'value': Decimal(value)/10**8,
             'block_id': block_id}

    dbsink.execute('submit_block_orig', stmt)


def submit_block_new(block_id, worker_name, date_found, block_hash, block_height, difficulty, value):
    stmt = '''
            INSERT INTO mining_block
               (id, founder, date_started, date_found, value, total_shares, total_score,
                difficulty, blocknum, is_mature, hash, workers)
            SELECT b.id + 1, '%(worker_name)s', b.date_found, '%(date_found)s', %(value)s, 0, 0,
                %(difficulty)s, %(block_height)s, -2, '%(block_hash)s', 0
            FROM mining_block b
            WHERE b.id = (select max(id) from mining_block);
            ''' % \
            {'worker_name': worker_name,
             'date_found': date_found,
             'block_hash': block_hash,
             'difficulty': difficulty,
             'block_height': block_height,
             'value': Decimal(value)/10**8}

    dbsink.execute('submit_block', stmt)


def increase_worker_blocks(worker_id):
    stmt = '''UPDATE mining_worker
             SET blocks=blocks+1 WHERE id=%s''' % worker_id

    dbsink.execute('increase_worker_blocks', stmt)


@defer.inlineCallbacks
def get_block_info_orig(create_new=True):
    """Returns 2-tuple (block_id, date_started)"""
    query = '''SELECT id, date_started FROM mining_block
            WHERE date_found = '%(null_date)s'
            ORDER BY id DESC LIMIT 1''' % \
            {'null_date': config.NULL_DATE}

    ret = (yield dbsink.direct_query('get_block_info_orig', query))
    try:
        # First row
        defer.returnValue(ret[0])
    except IndexError, e:
        if create_new:
            _create_new_block_orig()
        raise e


@defer.inlineCallbacks
def get_block_info_new(create_new=True):
    """Returns 2-tuple (block_id, date_started)"""
    query = '''SELECT id + 1, date_found FROM mining_block
               WHERE id = (select max(id) from mining_block)'''

    ret = (yield dbsink.direct_query('get_block_info', query))
    defer.returnValue(ret[0])


def switch_block_submission(drop_block_template=False):
    global submit_block, get_block_info
    submit_block = submit_block_new
    get_block_info = get_block_info_new

    if drop_block_template:
        _drop_block_template()


submit_block = submit_block_orig
get_block_info = get_block_info_orig
if not config.TEMPORARY__DATABASE_USE_BLOCK_TEMPALATE:
    switch_block_submission(False)


@defer.inlineCallbacks
def _drop_block_template():
    query = '''select max(id) from mining_block'''
    max_id = (yield dbsink.direct_query('drop_block_template--q', query))[0][0]

    stmt = '''DELETE FROM mining_block
              WHERE id = %(max_id)d
                  and date_found = '%(null_date)s' ''' % \
            {'max_id': max_id,
             'null_date': config.NULL_DATE}

    dbsink.execute('drop_block_template', stmt)


def get_workers_group(since, count, offset):
    query = '''
        select
            w.id,
            u.username,
            w.login_suffix,
            w.difficulty_suggested
        from auth_user u
        inner join mining_worker w on u.id = w.owner_id
        where
            last_change >= '%(since)s'
            and is_active = 1
        order by last_change asc
        limit %(count)s
        offset %(offset)s''' % {'since': since,
                                'count': count,
                                'offset': offset}

    return dbsink.direct_query('get_workers_group', query)
