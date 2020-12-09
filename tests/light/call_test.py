#!/usr/bin/env python3

# allow imports from parent directory
# source: https://stackoverflow.com/a/11158224
import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import eth_utils

from conflux.config import default_config
from conflux.filter import Filter
from conflux.rpc import RpcClient
from conflux.utils import sha3 as keccak, priv_to_addr
from test_framework.blocktools import create_transaction, encode_hex_0x
from test_framework.test_framework import ConfluxTestFramework
from test_framework.util import assert_equal, assert_is_hex_string, assert_is_hash_string
from test_framework.util import *

CONTRACT_PATH = "../contracts/EventsTestContract_bytecode.dat"
CONSTRUCTED_TOPIC = encode_hex_0x(keccak(b"Constructed(address)"))
FOO_TOPIC = encode_hex_0x(keccak(b"Foo(address,uint32)"))
NUM_CALLS = 20

FULLNODE0 = 0
FULLNODE1 = 1
LIGHTNODE = 2

class CallTest(ConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 3

        self.conf_parameters = {
            "log_level": "\"trace\"",
        }

    def setup_network(self):
        self.add_nodes(self.num_nodes)

        self.start_node(FULLNODE0, ["--archive"])
        self.start_node(FULLNODE1, ["--archive"])
        self.start_node(LIGHTNODE, ["--light"], phase_to_wait=None)

        # set up RPC clients
        self.rpc = [None] * self.num_nodes
        self.rpc[FULLNODE0] = RpcClient(self.nodes[FULLNODE0])
        self.rpc[FULLNODE1] = RpcClient(self.nodes[FULLNODE1])
        self.rpc[LIGHTNODE] = RpcClient(self.nodes[LIGHTNODE])

        # connect archive nodes
        connect_nodes(self.nodes, FULLNODE0, FULLNODE1)
        connect_nodes(self.nodes, LIGHTNODE, FULLNODE0)
        connect_nodes(self.nodes, LIGHTNODE, FULLNODE1)

        # wait for phase changes to complete
        self.nodes[FULLNODE0].wait_for_phase(["NormalSyncPhase"])
        self.nodes[FULLNODE1].wait_for_phase(["NormalSyncPhase"])

    def run_test(self):
        priv_key = default_config["GENESIS_PRI_KEY"]
        sender = eth_utils.encode_hex(priv_to_addr(priv_key))

        # deploy contract
        bytecode_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), CONTRACT_PATH)
        assert(os.path.isfile(bytecode_file))
        bytecode = open(bytecode_file).read()
        _, contract_addr = self.deploy_contract(sender, priv_key, bytecode)
        self.log.info(f"contract deployed: {contract_addr}")

        self.rpc[FULLNODE0].generate_blocks(20)
        sync_blocks(self.nodes)

        data_hex = encode_hex_0x(keccak(b"get_foo()"))
        x = self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full x = {x}")

        data_hex = encode_hex_0x(keccak(b"get_foo()"))
        x = self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light x = {x}")

        receipt = self.call_contract(sender, priv_key, contract_addr, encode_hex_0x(keccak(b"foo()")))
        self.rpc[FULLNODE0].generate_blocks(20)
        sync_blocks(self.nodes)

        data_hex = encode_hex_0x(keccak(b"get_foo()"))
        x = self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full x = {x}")

        data_hex = encode_hex_0x(keccak(b"get_foo()"))
        x = self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light x = {x}")

        data_hex = encode_hex_0x(keccak(b"delete_foo()"))
        x = self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full x = {x}")

        data_hex = encode_hex_0x(keccak(b"delete_foo()"))
        x = self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light x = {x}")

        # --------------------------------------

        data_hex = encode_hex_0x(keccak(b"get_mapping()"))
        x = self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full x = {x}")

        data_hex = encode_hex_0x(keccak(b"get_mapping()"))
        x = self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light x = {x}")

        data_hex = encode_hex_0x(keccak(b"delete_mapping()"))
        x = self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full x = {x}")

        data_hex = encode_hex_0x(keccak(b"delete_mapping()"))
        x = self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light x = {x}")

        data_hex = encode_hex_0x(keccak(b"destroy()"))
        self.rpc[FULLNODE0].call(contract_addr, data_hex)
        self.log.info(f"full destroyed")

        data_hex = encode_hex_0x(keccak(b"destroy()"))
        self.rpc[LIGHTNODE].call(contract_addr, data_hex)
        self.log.info(f"light destroyed")

        self.log.info("Pass")

    def deploy_contract(self, sender, priv_key, data_hex):
        tx = self.rpc[FULLNODE0].new_contract_tx(receiver="", data_hex=data_hex, sender=sender, priv_key=priv_key, storage_limit=2000)
        assert_equal(self.rpc[FULLNODE0].send_tx(tx, True), tx.hash_hex())
        receipt = self.rpc[FULLNODE0].get_transaction_receipt(tx.hash_hex())

        if receipt["outcomeStatus"] != "0x0":
            error = receipt["txExecErrorMsg"]
            self.log.info(f"Error deploying contract: {error}")
            assert(False)

        assert_equal(receipt["outcomeStatus"], "0x0")
        address = receipt["contractCreated"]
        assert_is_hex_string(address)
        return receipt, address

    def call_contract(self, sender, priv_key, contract, data_hex):
        tx = self.rpc[FULLNODE0].new_contract_tx(receiver=contract, data_hex=data_hex, sender=sender, priv_key=priv_key, storage_limit=1000)
        assert_equal(self.rpc[FULLNODE0].send_tx(tx, True), tx.hash_hex())
        receipt = self.rpc[FULLNODE0].get_transaction_receipt(tx.hash_hex())

        if receipt["outcomeStatus"] != "0x0":
            error = receipt["txExecErrorMsg"]
            self.log.info(f"Error calling contract: {error}")
            assert(False)

        return receipt

if __name__ == "__main__":
    CallTest().main()
