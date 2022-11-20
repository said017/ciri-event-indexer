from apibara import EventFilter, IndexerRunner, Info, NewEvents
from apibara.indexer import IndexerRunnerConfiguration
from starknet_py.contract import identifier_manager_from_abi
from starknet_py.utils.data_transformer import FunctionCallSerializer
from datetime import datetime
from starknet_py.net.full_node_client import FullNodeClient

indexer_id = "my-indexer"

rpc_client = FullNodeClient("https://starknet-goerli.apibara.com/", "testnet")


async def handle_events(info: Info, block_events: NewEvents):
    """Handle a group of events grouped by block."""
    print(f"Received events for block {block_events.block.number}")
    for event in block_events.events:
        print(event)

    events = [
        {"address": event.address, "data": event.data, "name": event.name}
        for event in block_events.events
    ]

    # Insert multiple documents in one call.
    await info.storage.insert_many("events", events)


async def run_indexer(server_url=None, mongo_url=None, restart=None):
    print("Starting Apibara indexer")

    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            apibara_url=server_url,
            apibara_ssl=True,
            storage_url=mongo_url,
        ),
        reset_state=restart,
        indexer_id=indexer_id,
        new_events_handler=handle_events,
    )

    # Create the indexer if it doesn't exist on the server,
    # otherwise it will resume indexing from where it left off.
    #
    # For now, this also helps the SDK map between human-readable
    # event names and StarkNet events.
    # ciri profile - 0x03ea63dc43f089f652bec64f2a13427bf95b84fd214b85c2e2cda1ff91259117
    # Briq NFT - 0x0266b1276d23ffb53d99da3f01be7e29fa024dd33cd7f7b1eb7a46c67891c9d0
    # event - Transfer
    # event for ciri - user_created
    runner.add_event_filters(
        filters=[
            EventFilter.from_event_name(
                name="Transfer",
                address="0x0266b1276d23ffb53d99da3f01be7e29fa024dd33cd7f7b1eb7a46c67891c9d0",
            )
        ],
        index_from_block=201_000,
    )

    print("Initialization completed. Entering main loopz.")

    await runner.run()

uint256_abi = {
    "name": "Uint256",
    "type": "struct",
    "size": 2,
    "members": [
        {"name": "low", "offset": 0, "type": "felt"},
        {"name": "high", "offset": 1, "type": "felt"},
    ],
}

transfer_abi = {
    "name": "Transfer",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "from_address", "type": "felt"},
        {"name": "to_address", "type": "felt"},
        {"name": "token_id", "type": "Uint256"},
    ],
}

transfer_decoder = FunctionCallSerializer(
    abi=transfer_abi,
    identifier_manager=identifier_manager_from_abi([
        transfer_abi, uint256_abi
    ]),
)

def decode_transfer_event(data):
    data = [int.from_bytes(b, "big") for b in data]
    return transfer_decoder.to_python(data)

def encode_int_as_bytes(n):
    return n.to_bytes(32, "big")

async def handle_events(info, block_events):
    # (Get Block Section)
    block = await rpc_client.get_block(
        block_hash=block_events.block.hash
    )
    block_time = datetime.fromtimestamp(block["accepted_time"])

    transfers = [
        decode_transfer_event(event.data)
        for event in block_events.events
    ]
    transfers_docs = [
        {
            "from_address": encode_int_as_bytes(tr.from_address),
            "to_address": encode_int_as_bytes(tr.to_address),
            "token_id": encode_int_as_bytes(tr.token_id),
            "timestamp": block_time,
        }
        for tr in transfers
    ]
    await info.storage.insert_many("transfers", transfers_docs)    

    new_token_owner = dict()
    for transfer in transfers:
        new_token_owner[transfer.token_id] = transfer.to_address

    for token_id, new_owner in new_token_owner.items():
        token_id = encode_int_as_bytes(token_id)
        await info.storage.find_one_and_replace(
            "tokens",
            {"token_id": token_id},
            {
                "token_id": token_id,
                "owner": encode_int_as_bytes(new_owner),
                "updated_at": block_time,
            },
            upsert=True,
        )    
