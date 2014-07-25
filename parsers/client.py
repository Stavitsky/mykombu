#!/usr/bin/python
# Filename: parsers.py

import argparse  # for parsing of argumenets
import sys


def create_parser():
    """Function creates parser of params"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--directory', default='img',
                        help='looking directory at virtual host')
    parser.add_argument('-f', '--file', default='test.json',
                        help='output or checking file')
    parser.add_argument('-u', '--uri',
                        default='amqp://guest:guest@localhost:5672//',
                        help='param of host connection')
    parser.add_argument('-p', '--peer', help='peer name to send message',
                        required=True)
    parser.add_argument('-c', '--command', choices=['check', 'write'],
                        default='write',
                        help='what action to do with qcow info')
    return parser

parser = create_parser()
namespace = parser.parse_args(sys.argv[1:])

host = format(namespace.uri)
peer = format(namespace.peer)
command = format(namespace.command)
currentpath = format(namespace.directory)
currentfile = format(namespace.file)

# End of parsers.py
