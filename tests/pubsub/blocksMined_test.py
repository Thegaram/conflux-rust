#!/usr/bin/env python3

# allow imports from parent directory
# source: https://stackoverflow.com/a/11158224
import os, sys, random
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import asyncio

from conflux.rpc import RpcClient
from conflux.pubsub import PubSubClient
from test_framework.test_framework import ConfluxTestFramework
from test_framework.util import assert_equal, connect_nodes, sync_blocks

FULLNODE0 = 0
FULLNODE1 = 1

NUM_BLOCKS = 50

class PubSubTest(ConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 2

    def setup_network(self):
        self.add_nodes(self.num_nodes)

        self.start_node(FULLNODE0, ["--archive"])
        self.start_node(FULLNODE1, ["--archive"])

        # set up RPC clients
        self.rpc = [None] * self.num_nodes
        self.rpc[FULLNODE0] = RpcClient(self.nodes[FULLNODE0])
        self.rpc[FULLNODE1] = RpcClient(self.nodes[FULLNODE1])

        # set up PubSub clients
        self.pubsub = [None] * self.num_nodes
        self.pubsub[FULLNODE0] = PubSubClient(self.nodes[FULLNODE0])
        self.pubsub[FULLNODE1] = PubSubClient(self.nodes[FULLNODE1])

        # connect nodes
        connect_nodes(self.nodes, FULLNODE0, FULLNODE1)

        # wait for phase changes to complete
        self.nodes[FULLNODE0].wait_for_phase(["NormalSyncPhase"])
        self.nodes[FULLNODE1].wait_for_phase(["NormalSyncPhase"])

    async def run_async(self):
        # subscribe
        sub_heads = [None] * 2
        sub_heads[FULLNODE0] = await self.pubsub[FULLNODE0].subscribe("newHeads")
        sub_heads[FULLNODE1] = await self.pubsub[FULLNODE1].subscribe("newHeads")

        sub_mined = [None] * 2
        sub_mined[FULLNODE0] = await self.pubsub[FULLNODE0].subscribe("blocksMined")
        sub_mined[FULLNODE1] = await self.pubsub[FULLNODE1].subscribe("blocksMined")

        for _ in range(NUM_BLOCKS):
            node = random.randint(0, 1)
            other_node = 1  - node

            hash = self.rpc[node].generate_block()
            self.log.info(f"Generated {hash} on node-{node}")

            sync_blocks(self.nodes)



            #
            block = await sub_heads[node].next()
            assert_equal(block["hash"], hash)

            block = await sub_heads[other_node].next()
            assert_equal(block["hash"], hash)

            #
            block = await sub_mined[node].next()
            assert_equal(block["hash"], hash)
            self.log.info(f"Mined {hash} on node-{node}")

            try:
                block = await sub_mined[other_node].next(timeout=0.1)
                assert(False)
            except TimeoutError:
                pass

        await sub_heads[FULLNODE0].unsubscribe()
        await sub_heads[FULLNODE1].unsubscribe()
        await sub_mined[FULLNODE0].unsubscribe()
        await sub_mined[FULLNODE1].unsubscribe()

        self.log.info("Pass")

    def run_test(self):
        asyncio.get_event_loop().run_until_complete(self.run_async())

if __name__ == "__main__":
    PubSubTest().main()
