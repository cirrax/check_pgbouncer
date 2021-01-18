#! /usr/bin/env python3

"""Hello world Nagios check."""

import argparse
import nagiosplugin
import logging
import psycopg2

_version = "0.1.0"
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class PgBouncer(nagiosplugin.Resource):
    """extract stats from pgbouncer"""

    def __init__(self, connectionstring, pm):
        self.connectionstring = connectionstring
        self.pm = pm

    def probe(self):
        logging.debug('connect to pgbouncer')
        try:
            conn = psycopg2.connect(self.connectionstring)
        except:
            print("PGBOUNCER UNKNOWN - Unable to connect")
            exit(3)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        # SHOW HELP|CONFIG|DATABASES|POOLS|CLIENTS|SERVERS|VERSION
        # SHOW STATS|FDS|SOCKETS|ACTIVE_SOCKETS|LISTS|MEM|DNS_HOSTS|DNS_ZONES
        cur.execute("SHOW CONFIG")
        config = cur.fetchall()
        cur.execute("SHOW DATABASES")
        databases = cur.fetchall()
        # name,host,port,database,force_user,pool_size,reserve_pool,pool_mode,max_connections,current_connections
        cur.execute("SHOW POOLS")
        pools = cur.fetchall()
        # database,user,cl_active,cl_waiting,sv_active,sv_idle,sv_used,sv_tested,svloggingin,maxwait,pool_mode
        logging.debug('close connection')

        # clients
        max_clients = int([item[1] for item in config if item[0] == "max_client_conn"][0])
        cur_clients = sum([item[2] for item in pools])
        pct_clients = 100 * float(cur_clients) / max_clients  # percentage of max_client_conn
        # maxdb
        top_db = max(databases, key=lambda x: float(x[9])/x[8])  # find database with highest conn percentage used
        pct_db = 100 * float(top_db[9]) / top_db[8]
        # maxpool
        top_pool = max(pools, key=lambda x: float(x[4])/([i[5] for i in databases if i[0] == x[0]][0]))
        pct_pool = 100 * float(top_pool[4])/[i[5] for i in databases if i[0] == top_pool[0]][0]
        # maxwait
        max_wait = max(pools, key=lambda x: x[9])  # find pool with longest wait for server connections

        logging.debug(top_db)
        logging.debug( "Clients: %0.2f%%, MaxDB: %0.2f%%, MaxPool %0.2f%%, MaxWait %0.2fs", pct_clients, pct_db, pct_pool, max_wait[9])


        if self.pm == 'clients':
            return [nagiosplugin.Metric('Clients', pct_clients, uom='%', min=0, max=100)]
        elif self.pm == 'maxdb':
            return [nagiosplugin.Metric('MaxDB', pct_db, uom='%', min=0, max=100)]
        elif self.pm == 'maxpool':
            return [nagiosplugin.Metric('MaxPool', pct_pool, uom='%', min=0, max=100)]
        elif self.pm == 'maxwait':
            return [nagiosplugin.Metric('MaxWait', max_wait[9], uom='s', min=0, max=100)]
        else:
            return [
                nagiosplugin.Metric('Clients', pct_clients, uom='%', min=0, max=100),
                nagiosplugin.Metric('MaxDB', pct_db, uom='%', min=0, max=100),
                nagiosplugin.Metric('MaxPool', pct_pool, uom='%', min=0, max=100),
                nagiosplugin.Metric('MaxWait', max_wait[9], uom='s', min=0, max=1.0)
            ]


class PgBouncerSummary(nagiosplugin.Summary):
    """Status line for pgbouncer information.
    """

    def ok(self, results):
        _text = ''
        for res in results:
            _text = _text + str(res) + ', '
        return _text

    def problem(self, results):
        _text = ''
        for res in results:
            _text = _text + str(res) + ' '
        return _text


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description="nagios check for pgbouncer")

    argp.add_argument('-V', '--version', action='version', version='%(prog)s ' + _version )
    argp.add_argument('-t', '--timeout', type=int, metavar="SEC", default=23, help="check timeout in seconds")
    argp.add_argument('-w', '--warning', metavar='LIST', type=str, default='75', help="warning thresholds")
    argp.add_argument('-c', '--critical', metavar='LIST', type=str, default='90', help="critical thresholds")
    argp.add_argument('-H', '--hostname', metavar='HOST', help="domain-name or ip-number")
    argp.add_argument('-v', '--verbose', action='count', default=0, help="up to 3 times")
    argp.add_argument('-a', '--authentication', metavar='PASS', help="authentication password")
    argp.add_argument('-l', '--logname', metavar='USER',help="login name")
    argp.add_argument('-p', '--port', type=int, help="IP port number")
    argp.add_argument('-d', '--database', metavar='conn', help='connect string to pgbouncer pseudo database')
    argp.add_argument('-m', '--mode',  help='check-mode (clients|maxpool|maxdb|maxwait)')
    args = argp.parse_args()
    logging.debug(args)

    warn_list = [75.0, 75.0, 75.0, 0.1]
    c = 0
    for item in args.warning.split(','):
        warn_list[c] = float(item)
        c += 1
    crit_list = [90.0, 90.0, 90.0, 0.2]
    c = 0
    for item in args.critical.split(','):
        crit_list[c] = float(item)
        c += 1

    #if args.version:
    #    print('Version: ' + _version)
    #    exit(0)

    if args.database:
        connstring = args.database
    else:
        connstring = 'dbname=pgbouncer'
        if args.hostname:
            connstring += ' host=' + args.hostname
        if args.port:
            connstring += ' port=' + str(args.port)
        if args.authentication:
            connstring += ' password=' + args.authentication
        if args.logname:
            connstring += ' user=' + args.logname

    connstring = connstring + ' connect_timeout=' + str(args.timeout // 2)
    logging.debug(connstring)

    check = nagiosplugin.Check(
        PgBouncer(connstring, args.mode),
        nagiosplugin.ScalarContext('Clients', warn_list[0], crit_list[0]),
        nagiosplugin.ScalarContext('MaxDB', warn_list[1], crit_list[1]),
        nagiosplugin.ScalarContext('MaxPool', warn_list[2], crit_list[2]),
        nagiosplugin.ScalarContext('MaxWait', warn_list[3], crit_list[3]),
        PgBouncerSummary()
    )
    check.main(args.verbose, args.timeout)


if __name__ == '__main__':
    main()
