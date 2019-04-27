#!/usr/bin/env python3

import argparse
import binascii

from neo.Storage.Implementation.DBMigrate import migrateDB

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--from-db", action="store",
                        help="Backend you want to migrate from [leveldb]")
    parser.add_argument("--to-db", action="store",
                        help="Backend you want to migrate to [rocksdb]")
    parser.add_argument("--data-dir", action="store",
                        help="Absolute path to use for the source database \
                              directories [/home/USER/.neopython/fixtures/\
                              test_chain]")

    args = parser.parse_args()

    if not all(list(vars(args).values())):
        print('\nAll arguments need to be given\n')
        parser.print_help()
        exit(2)

    migrateDB(fromdb=args.from_db, todb=args.to_db, 
             path=args.data_dir, remove_old=False)    

if __name__ == "__main__":
    main()
