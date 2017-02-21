"""
    CSV to Redis loader file
    It takes an input CSV file and outputs the data into Redis.

    Usage : csv-to-redis.py -i <input-csv-file> -o <output-file> -k <key-set> -f <key-fieldname>

    The input-csv-file is self-explanatory

    The output-file is used to log any errors or warnings that may occur. It is a good idea to read this
    file and look for any duplicates that have been rejected by Redis

    The key-set is the hash-key for the data. For example if the CSV file contains user data, then the key-set for the
    first row in the CSV could be userdata1 and userdata2 for the second row and so on

    The <key-fieldname> identifies an unique set of keys for each of the rows in the CSV file. For example the userId
    may be the unique key. It checks to see that there are no rows in the CSV file that have duplicate userId. If there
    exist duplicates they are rejected and added to the output file
"""

import sys
import getopt
import redis
import csv
import logging

headers = None
logger = None
HOST = "localhost"
PORT = 6379
DB = 0


def get_logger(logfile):
    global logger
    logger = logging.getLogger(__name__)
    hdlr = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)


def _get_connection():
    """
    Create and return a Redis connection. Returns None on failure.
    """
    try:
        pool = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
        conn = redis.Redis(connection_pool=pool)
        return conn
    except redis.RedisError as e:
        logger.error(e)

    return None


def add_redis_set_key(conn, k, v):
    # Check if key is unique in the set , if not you can do the hmset below, else skip
    dup_keyfield = False
    try:
        dup_keyfield = conn.sadd(k, v)
        if dup_keyfield is False:
            logger.warning("{} field has duplicate value of {}".format(k, v))
    except redis.RedisError as e:
        logger.error(e)

    return not dup_keyfield


def add_redis_hashmap(pipe, keyset, key, value):
    try:
        pipe.hmset(keyset, {key: value})
    except redis.RedisError as e:
        logger.error(e)


def to_dict(values):
    d = {}
    global headers
    for i in range(len(headers)):
        d[headers[i]] = values[i]
    return d

def read_data(reader):
    for row in reader:
        yield row

def convert_file(inputfile, keyfield, keyset):
    count = 0
    try:
        with open(inputfile) as f:
            csv.register_dialect('escaped', delimiter=",", escapechar="\\")
            reader = csv.DictReader(f, dialect='escaped')
            conn = _get_connection()
            pipe = conn.pipeline(False)

            for row in read_data(reader):
                valuesdict = {}
                count += 1
                hashkeyset = ''.join([keyset, str(count)])
                for key, value in row.items():
                    if key == keyfield:
                        dupkeyfield = add_redis_set_key(conn, keyfield, value)
                        if dupkeyfield is True:
                            valuesdict.clear()
                            break
                    valuesdict[key] = value
                for k, v in valuesdict.items():
                    add_redis_hashmap(pipe, hashkeyset, k, v)
            pipe.execute()
            f.close()
    except IOError as e:
        assert isinstance(e, object)
        print("I/O error opening file {0}: {1} {2}".format(inputfile, e.errno, e.strerror))


def usage():
    print('csv-to-redis.py -i <input-csv-file> -o <output-file> -k <key-set> -f <key-fieldname>')


if __name__ == '__main__':
    inputfile = ""
    outputfile = ""
    keyfield = ""
    keyset = ""

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:o:f:k:", ["ifile=", "ofile=", "keyfield=", "keyset"])
        print('opts=', opts)
        print('args=', args)
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("-f", "--keyfield"):
            keyfield = arg
        elif opt in ("-k", "--keyset"):
            keyset = arg

    if not inputfile:
        print("ERROR: -i is missing")
        usage()
        sys.exit(2)

    if not outputfile:
        print("ERROR: -l is missing")
        usage()
        sys.exit(2)

    if not keyfield:
        print("ERROR: -f is missing")
        usage()
        sys.exit(2)

    if not keyset:
        print("ERROR: -k is missing")
        usage()
        sys.exit(2)

    get_logger(outputfile)
    convert_file(inputfile, keyfield, keyset)
