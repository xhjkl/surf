# 🌊 surf

A data aggregator. It continuously indexes the Solana blockchain and
provides a web interface to query the data.

## Usage

```bash
cargo run --release
```

You can also pass `--host` and `--port` for the web interface to bind to,
and `--url` to connect to a different Solana RPC node.

## Endpoints

While running, the aggregator exposes an HTTP API.

### `GET /`

Returns a short description of the service.

### `GET /blockheight`

The greatest block index the aggregator has seen so far.

### `GET /votes`

A list of all the vote transactions.

### `GET /transfers`

A list of all SOL transfers.

## Query Parameters

To query not all, but some of the data, you can use the query parameters
for the `/votes` and `/transfers` endpoints:
  - `signature`: The concrete signature of the transaction.
  - `block`: The block index of the block containing the transaction.
  - `to`: The target of the vote transaction or the recipient of the transfer.
  - `from`: The author of the vote transaction or the sender of the transfer.

That is, ```/votes?to=1e1e1e1``` will return all votes that the given address received.
