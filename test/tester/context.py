from inspect import getframeinfo, stack
import clickhouse_connect
import socket
import time


def get_service(name):
    services = {
        "stilgar": 8080,
        "clickhouse": 9100,
    }

    try:
        return services[name]
    except KeyError:
        raise Exception("Unknown test service: %s" % name)


def get_service_url(name, *args):
    url_path = '/' + '/'.join(args)
    return "http://%s:%d%s" % (name, get_service(name), url_path)


def wait_for_all():
    while True:
        all_up = True
        for service in ('stilgar', 'clickhouse'):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                parser_port = get_service(service)
                sock.connect((service, parser_port))
                sock.close()
            except socket.error:
                all_up = False
        if all_up:
            return
        time.sleep(1)


Clickhouse = None


def init_clickhouse():
    global Clickhouse
    Clickhouse = clickhouse_connect.get_client(
        host='clickhouse',
        username='myuser',
        password='mypassword',
        database='mydatabase'
    )


def query(sql, **kwargs):
    entries = Clickhouse.query(sql, **kwargs)
    return [dict(zip(entries.column_names, row)) for row in entries.result_rows]


def get_all(table):
    return query("SELECT * FROM %s" % table)


def reset():
    for tbl in ["aliases", "pages", "screens", "identifies", "tracks", "groups"]:
        Clickhouse.command("TRUNCATE TABLE IF EXISTS %s" % tbl)
    for tbl, col in [("pages", "expandable_property"), ("pages", "expanded_property")]:
        Clickhouse.command("ALTER TABLE %s DROP COLUMN IF EXISTS %s" % (tbl, col))
    for tbl in ["custom_test_event", "track_2fa", "custom_event_invalid_identifier", "max_table_expansion_test", "max_table_width_test"]:
        Clickhouse.command("DROP TABLE IF EXISTS %s" % tbl)


def assert_many_equals(test_cases):
    caller = getframeinfo(stack()[1][0])
    for name, expected, actual in test_cases:
        assert expected == actual, "expected %s %r, got %r (%s:%d)" % (name, expected, actual, caller.filename, caller.lineno)
