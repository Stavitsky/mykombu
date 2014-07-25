import argparse  # for parsing of argumenets
import sys


def create_parser():
    """Function creates parser of params"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--peer', help='server name & routing key',
                        required=True)
    return parser

parser = create_parser()
namespace = parser.parse_args(sys.argv[1:])

sname = format(namespace.peer)

# End of parsers.py
