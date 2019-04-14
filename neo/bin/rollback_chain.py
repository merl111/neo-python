#!/usr/bin/env python3
import sys
import argparse
from neo.Settings import settings
from neo.Core.Blockchain import Blockchain
from neo.Storage.Implementation.DBFactory import getBlockchainDB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mainnet", action="store_true", default=False,
                        help="use MainNet instead of the default TestNet")
    parser.add_argument("-c", "--config", action="store", help="Use a specific config file")

    # Where to store stuff
    parser.add_argument("--datadir", action="store",
                        help="Absolute path to use for database directories")

    parser.add_argument("-b", "--block-id", type=int, help="The block id to rollback to")

    args = parser.parse_args()

    if args.mainnet and args.config:
        print("Cannot use both --config and --mainnet parameters, please use only one.")
        exit(1)

    # Setting the datadir must come before setting the network, else the wrong path is checked at net setup.
    if args.datadir:
        settings.set_data_dir(args.datadir)

    # Setup depending on command line arguments. By default, the testnet settings are already loaded.
    if args.config:
        settings.setup(args.config)
    elif args.mainnet:
        settings.setup_mainnet()

    # Instantiate the blockchain and subscribe to notifications
    blockchain = Blockchain(getBlockchainDB())
    Blockchain.RegisterBlockchain(blockchain)
    blockchain.RollbackChain(args.block_id)


if __name__ == "__main__":
    main()
