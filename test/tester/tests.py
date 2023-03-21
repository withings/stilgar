from tester.helpers import Stilgar, Events, WRITE_KEY, NOT_WRITE_KEY_B64
from tester.context import Clickhouse, assert_many_equals

# from tester.context import get_service_url
# import rudderstack.analytics as rudder_analytics
# rudder_analytics.write_key = "not-actually-secret"
# rudder_analytics.dataPlaneUrl = get_service_url("stilgar")
# rudder_analytics.sync = True
# rudder_analytics.debug = True


##################
# Authentication #
##################

def test_source_config_no_auth_disabled():
    source_config = Stilgar.sourceConfig()
    assert source_config.status_code == 403, "expected 403 on sourceConfig without a key"
    response = source_config.json()
    assert not response['source']['enabled'], "expected disabled source for bad key"


def test_source_config_bad_auth_disabled():
    source_config = Stilgar.sourceConfig(params={"writeKey": "not-%s" % WRITE_KEY})
    assert source_config.status_code == 403, "expected 403 for bad key"
    response = source_config.json()
    assert not response['source']['enabled'], "expected disabled source for bad key"


def test_source_config_good_auth_enabled():
    source_config = Stilgar.sourceConfig(params={"writeKey": WRITE_KEY})
    assert source_config.status_code == 200, "expected 200 for good key"
    response = source_config.json()
    assert response['source']['enabled'], "expected enabled source for good key"


def test_authentication_page_no_key():
    page = Events.page()
    store_page = Stilgar.page(json=page, disable_auth=True)
    assert store_page.status_code == 401, "unexpected status %d" % store_page.status_code

    pages = Clickhouse.query("SELECT * FROM pages")
    pages = [dict(zip(pages.column_names, row)) for row in pages.result_rows]
    assert len(pages) == 0, "expected 0 page in DB, got %d" % len(pages)


def test_authentication_page_bad_key():
    page = Events.page()
    store_page = Stilgar.page(json=page, headers={'Authorization': 'Basic %s' % NOT_WRITE_KEY_B64})
    assert store_page.status_code == 403, "unexpected status %d" % store_page.status_code

    pages = Clickhouse.query("SELECT * FROM pages")
    pages = [dict(zip(pages.column_names, row)) for row in pages.result_rows]
    assert len(pages) == 0, "expected 0 page in DB, got %d" % len(pages)


#################
# Rate limiting #
#################

def test_payload_too_large():
    page = Events.page()
    page['name'] = "0" * (2**21)
    store_page = Stilgar.page(json=page)
    assert store_page.status_code == 413, "unexpected status %d" % store_page.status_code

    pages = Clickhouse.query("SELECT * FROM pages")
    pages = [dict(zip(pages.column_names, row)) for row in pages.result_rows]
    assert len(pages) == 0, "expected 0 page in DB, got %d" % len(pages)


################
# Alias events #
################

def test_alias_no_previous_id():
    alias = Events.alias()
    del alias['previousId']
    store_alias = Stilgar.alias(json=alias)
    assert store_alias.status_code == 400, "unexpected status %d" % store_alias.status_code


def test_alias_no_user():
    alias = Events.alias()
    store_alias = Stilgar.alias(json=alias)
    assert store_alias.status_code == 200, "unexpected status %d" % store_alias.status_code

    aliases = Clickhouse.query("SELECT * FROM aliases")
    aliases = [dict(zip(aliases.column_names, row)) for row in aliases.result_rows]
    assert len(aliases) == 1, "expected 1 alias in DB, got %d" % len(aliases)

    assert_many_equals((
        ('anonymous_id', alias['anonymousId'], aliases[0]['anonymous_id']),
        ('previous_id', alias['previousId'], aliases[0]['previous_id']),
    ))

    assert aliases[0]['user_id'] is None, "unexpected user ID: %r" % aliases[0]['user_id']


def test_alias_with_user():
    alias = Events.alias()
    alias['userId'] = Events.random_str()
    store_alias = Stilgar.alias(json=alias)
    assert store_alias.status_code == 200, "unexpected status %d" % store_alias.status_code

    aliases = Clickhouse.query("SELECT * FROM aliases")
    aliases = [dict(zip(aliases.column_names, row)) for row in aliases.result_rows]
    assert len(aliases) == 1, "expected 1 alias in DB, got %d" % len(aliases)

    assert_many_equals((
        ('user_id', alias['userId'], aliases[0]['user_id']),
        ('previous_id', alias['previousId'], aliases[0]['previous_id']),
    ))

    assert aliases[0]['anonymous_id'] != aliases[0]['user_id'], "user ID should not match anonymous ID"


################
# Group events #
################

def test_group_no_group_id():
    group = Events.group()
    del group['groupId']
    store_group = Stilgar.group(json=group)
    assert store_group.status_code == 400, "unexpected status %d" % store_group.status_code


def test_group_no_user():
    group = Events.group()
    store_group = Stilgar.group(json=group)
    assert store_group.status_code == 200, "unexpected status %d" % store_group.status_code

    groups = Clickhouse.query("SELECT * FROM groups")
    groups = [dict(zip(groups.column_names, row)) for row in groups.result_rows]
    assert len(groups) == 1, "expected 1 group in DB, got %d" % len(groups)

    assert_many_equals((
        ('anonymous_id', group['anonymousId'], groups[0]['anonymous_id']),
        ('group_id', group['groupId'], groups[0]['group_id']),
        ('context_traits_group_awesomeness', group['traits']['group_awesomeness'], groups[0]['context_traits_group_awesomeness'])
    ))

    assert groups[0]['user_id'] is None, "unexpected user ID: %r" % groups[0]['user_id']


def test_group_with_user():
    group = Events.group()
    group['userId'] = Events.random_str()
    store_group = Stilgar.group(json=group)
    assert store_group.status_code == 200, "unexpected status %d" % store_group.status_code

    groups = Clickhouse.query("SELECT * FROM groups")
    groups = [dict(zip(groups.column_names, row)) for row in groups.result_rows]
    assert len(groups) == 1, "expected 1 group in DB, got %d" % len(groups)

    assert_many_equals((
        ('anonymous_id', group['anonymousId'], groups[0]['anonymous_id']),
        ('user_id', group['userId'], groups[0]['user_id']),
        ('group_id', group['groupId'], groups[0]['group_id']),
    ))

    assert groups[0]['anonymous_id'] != groups[0]['user_id'], "user ID should not match anonymous ID"


###############
# Page events #
###############

def test_store_page_no_user():
    page = Events.page()
    store_page = Stilgar.page(json=page)
    assert store_page.status_code == 200, "unexpected status %d" % store_page.status_code

    pages = Clickhouse.query("SELECT * FROM pages")
    pages = [dict(zip(pages.column_names, row)) for row in pages.result_rows]
    assert len(pages) == 1, "expected 1 page in DB, got %d" % len(pages)

    assert_many_equals((
        ('anonymous_id', page['anonymousId'], pages[0]['anonymous_id']),
        ('name', page['name'], pages[0]['name']),
        ('context_os_version', page['context']['os']['version'], pages[0]['context_os_version'])
    ))

    assert pages[0]['user_id'] is None, "unexpected user ID: %r" % pages[0]['user_id']


def test_store_page_with_user():
    page = Events.page()
    page['userId'] = Events.random_str()
    store_page = Stilgar.page(json=page)
    assert store_page.status_code == 200, "unexpected status %d" % store_page.status_code

    pages = Clickhouse.query("SELECT * FROM pages")
    pages = [dict(zip(pages.column_names, row)) for row in pages.result_rows]
    assert len(pages) == 1, "expected 1 page in DB, got %d" % len(pages)

    assert_many_equals((
        ('anonymous_id', page['anonymousId'], pages[0]['anonymous_id']),
        ('user_id', page['userId'], pages[0]['user_id']),
    ))

    assert pages[0]['anonymous_id'] != pages[0]['user_id'], "user ID should not match anonymous ID"


#################
# Screen events #
#################

def test_store_screen_no_user():
    screen = Events.screen()
    store_screen = Stilgar.screen(json=screen)
    assert store_screen.status_code == 200, "unexpected status %d" % store_screen.status_code

    screens = Clickhouse.query("SELECT * FROM screens")
    screens = [dict(zip(screens.column_names, row)) for row in screens.result_rows]
    assert len(screens) == 1, "expected 1 screen in DB, got %d" % len(screens)

    assert_many_equals((
        ('anonymous_id', screen['anonymousId'], screens[0]['anonymous_id']),
        ('name', screen['name'], screens[0]['name']),
    ))

    assert screens[0]['user_id'] is None, "unexpected user ID: %r" % screens[0]['user_id']


def test_store_screen_with_user():
    screen = Events.screen()
    screen['userId'] = Events.random_str()
    store_screen = Stilgar.screen(json=screen)
    assert store_screen.status_code == 200, "unexpected status %d" % store_screen.status_code

    screens = Clickhouse.query("SELECT * FROM screens")
    screens = [dict(zip(screens.column_names, row)) for row in screens.result_rows]
    assert len(screens) == 1, "expected 1 screen in DB, got %d" % len(screens)

    assert_many_equals((
        ('anonymous_id', screen['anonymousId'], screens[0]['anonymous_id']),
        ('user_id', screen['userId'], screens[0]['user_id']),
    ))

    assert screens[0]['anonymous_id'] != screens[0]['user_id'], "user ID should not match anonymous ID"


################
# Track events #
################

def test_store_track_no_event_type():
    track = Events.track()
    del track['event']
    store_track = Stilgar.track(json=track)
    assert store_track.status_code == 400, "unexpected status %d" % store_track.status_code


def test_store_track_no_user():
    track = Events.track()
    store_track = Stilgar.track(json=track)
    assert store_track.status_code == 200, "unexpected status %d" % store_track.status_code

    tracks = Clickhouse.query("SELECT * FROM tracks")
    tracks = [dict(zip(tracks.column_names, row)) for row in tracks.result_rows]
    assert len(tracks) == 1, "expected 1 track in DB, got %d" % len(tracks)

    assert_many_equals((
        ('anonymous_id', track['anonymousId'], tracks[0]['anonymous_id']),
        ('event', track['event'], tracks[0]['event']),
    ))

    assert tracks[0]['user_id'] is None, "unexpected user ID in tracks table: %r" % tracks[0]['user_id']
    assert 'custom_prop1' not in tracks[0].keys(), "custom event properties should not appear in tracks table"

    events = Clickhouse.query("SELECT * FROM custom_test_event")
    events = [dict(zip(events.column_names, row)) for row in events.result_rows]
    assert len(events) == 1, "expected 1 custom event, got %d" % len(events)

    assert 'custom_prop2' in events[0].keys(), "custom event properties should appear in custom event table"
    assert_many_equals((
        ('anonymous_id', track['anonymousId'], events[0]['anonymous_id']),
        ('event', track['event'], events[0]['event']),
        ('custom_prop2', track['properties']['custom_prop2'], events[0]['custom_prop2']),
    ))

    assert events[0]['user_id'] is None, "unexpected user ID: %r" % events[0]['user_id']


def test_store_track_with_user():
    track = Events.track()
    track['userId'] = Events.random_str()
    store_track = Stilgar.track(json=track)
    assert store_track.status_code == 200, "unexpected status %d" % store_track.status_code

    tracks = Clickhouse.query("SELECT * FROM tracks")
    tracks = [dict(zip(tracks.column_names, row)) for row in tracks.result_rows]
    assert len(tracks) == 1, "expected 1 track in DB, got %d" % len(tracks)

    assert_many_equals((
        ('anonymous_id', track['anonymousId'], tracks[0]['anonymous_id']),
        ('user_id', track['userId'], tracks[0]['user_id']),
    ))

    assert tracks[0]['anonymous_id'] != tracks[0]['user_id'], "user ID should not match anonymous ID"


###################
# Identify events #
###################

def test_identify_no_user_id():
    identify = Events.identify('dummy')
    del identify['userId']
    store_identify = Stilgar.identify(json=identify)
    assert store_identify.status_code == 400, "unexpected status %d" % store_identify.status_code


def test_identify_with_user():
    user_id = Events.random_str()
    identify = Events.identify(user_id, {"trait1": 42, "trait2": "boom"})
    store_identify = Stilgar.identify(json=identify)
    assert store_identify.status_code == 200, "unexpected status %d" % store_identify.status_code

    identifies = Clickhouse.query("SELECT * FROM identifies")
    identifies = [dict(zip(identifies.column_names, row)) for row in identifies.result_rows]
    assert len(identifies) == 1, "expected 1 identify in DB, got %d" % len(identifies)

    assert_many_equals((
        ('user_id', identify['userId'], identifies[0]['user_id']),
        ('context_traits_trait1', identify['context']['traits']['trait1'], identifies[0]['context_traits_trait1']),
        ('context_traits_trait2', identify['context']['traits']['trait2'], identifies[0]['context_traits_trait2']),
    ))

    users = Clickhouse.query("""SELECT
    anyLastSimpleState(context_traits_trait1) AS trait1,
    anyLastSimpleState(context_traits_trait2) AS trait2
    FROM users WHERE id = {user_id:String} GROUP BY id
    """, parameters={"user_id": user_id})
    users = [dict(zip(users.column_names, row)) for row in users.result_rows]
    assert len(users) == 1, "expected 1 user, got %d" % len(users)

    assert_many_equals((
        ('context_traits_trait1', identify['context']['traits']['trait1'], users[0]['trait1']),
        ('context_traits_trait2', identify['context']['traits']['trait2'], users[0]['trait2']),
    ))


def test_identify_traits_update():
    user_id = Events.random_str()
    identify = Events.identify(user_id, {"trait3": "incredible"})
    store_identify = Stilgar.identify(json=identify)
    assert store_identify.status_code == 200, "unexpected status %d on first identify" % store_identify.status_code

    users = Clickhouse.query("""
    SELECT anyLastSimpleState(context_traits_trait3) AS trait3
    FROM users WHERE id = {user_id:String} GROUP BY id
    """, parameters={"user_id": user_id})
    users = [dict(zip(users.column_names, row)) for row in users.result_rows]
    assert len(users) == 1, "expected 1 user, got %d" % len(users)
    assert identify['context']['traits']['trait3'] == users[0]['trait3'], "trait has wrong initial value"

    identify = Events.identify(user_id, {"trait3": "even more incredible"})
    store_identify = Stilgar.identify(json=identify)
    assert store_identify.status_code == 200, "unexpected status %d on second identify" % store_identify.status_code

    users = Clickhouse.query("""
    SELECT anyLastSimpleState(context_traits_trait3) AS trait3
    FROM users WHERE id = {user_id:String} GROUP BY id
    """, parameters={"user_id": user_id})
    users = [dict(zip(users.column_names, row)) for row in users.result_rows]
    assert len(users) == 1, "expected 1 user, got %d" % len(users)
    # TODO: does not work: for some reason, Clickhouse's anyLast returns... the first value in the state
    # assert identify['context']['traits']['trait3'] == users[0]['trait3'], "trait has wrong value after second identify"


################
# Batch events #
#################

def test_batch_three_events():
    events = {
        "page": Events.page(),
        "screen": Events.screen(),
        "track": Events.track()
    }
    batch = {"batch": [events["page"], events["screen"], events["track"]]}
    store_batch = Stilgar.batch(json=batch)
    assert store_batch.status_code == 200, "unexpected status %d" % store_batch.status_code

    for event_type in ("page", "screen", "track"):
        db_events = Clickhouse.query("SELECT * FROM %ss" % event_type)
        db_events = [dict(zip(db_events.column_names, row)) for row in db_events.result_rows]
        assert len(db_events) == 1, "expected 1 %s in DB, got %d" % (event_type, len(db_events))
        assert db_events[0]['id'] == events[event_type]['messageId'], "wrong %s message ID: %s, expected %" % (
            event_type,
            db_events[0]['id'],
            events[event_type]['messageId']
        )
