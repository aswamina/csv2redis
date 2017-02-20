import sys
import getopt
import redis
import csv
import logging

ndups = 0
headers = None
logger = None
HOST = "localhost"
PORT = 6379
DB = 0


def getLogger(logfile):
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


def addRedisSetKey(conn, d, k):
    # TODO : Check if key is unique in the set , if not you can do the hmset below, else skip
    dup_keyfield = False
    try:
        dup_keyfield = conn.sadd(k, d[k])
        if dup_keyfield == False:
            logger.warning("{} field has duplicate value of {}".format(k, d[k]))
    except redis.RedisError as e:
        logger.error(e)

    return dup_keyfield

def addRedisHashMap(conn, keyset, key, value):
    try:
        conn.hmset(keyset, {key: value})
    except redis.RedisError as e:
        logger.error(e)


def to_dict(values):
    d = {}
    global headers
    for i in range(len(headers)):
        d[headers[i]] = values[i]
    return d


def convert_file(inputfile, keyfield, keyset):
    count = 0

    try:
        with open(inputfile) as f:
            reader = csv.reader(f, delimiter=',', quotechar='\')
            for line in reader:
                count = count + 1
                values = line
                if count == 1:
                    global headers
                    headers = values
                else:
                    conn = _get_connection()
                    hashkeyset = ''.join([keyset, str(count - 1)])
                    valuesDict = to_dict(values)
                    if (addRedisSetKey(conn, valuesDict, keyfield) == True):
                        for key, value in valuesDict.items():
                            addRedisHashMap(conn, hashkeyset, key, value)
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

    getLogger(outputfile)
    convert_file(inputfile, keyfield, keyset)
