WITH data AS (
SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_ethereum.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'ethereum'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_polygon.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'polygon'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_optimism.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'optimism'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_arbitrum.L2Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'arbitrum'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_avalanche_c.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'avalanche_c'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_fantom.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'fantom'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY

UNION ALL

SELECT
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_base.L2Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'base'
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY
),

p AS (
SELECT
    date_trunc('{{Time interval}}', minute) as day,
    blockchain,
    contract_address,
    AVG(price) AS avg_price
FROM prices.usd
WHERE minute > NOW() - interval '{{Trading Num Days}}' DAY
GROUP BY 1, 2, 3
)

SELECT
    data.day,
    data.blockchain,
    SUM(amount*avg_price) AS volume,
    COUNT(DISTINCT users) AS users,
    COUNT(DISTINCT txs) AS txs
FROM data
LEFT JOIN p ON data.day = p.day AND p.contract_address = data.contract_address AND p.blockchain = data.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC