-- This query aggregates Aave V3 deposit activity across multiple blockchains, converts token deposits into USD volume using average prices,
-- and measures daily liquidity, users, and transactions by chain.

-- Cette requête agrège l’activité de dépôts sur Aave V3 sur plusieurs blockchains, convertit les montants déposés en volume USD grâce aux prix moyens, 
-- puis mesure la liquidité, les utilisateurs et les transactions par chaîne.

-- build a unified dataset of Aave v3 deposit events across multiple blockchain
WITH data AS (
SELECT -- ETH deposits
    date_trunc('{{Time interval}}', evt_block_time) as day,
    blockchain,
    t.symbol as token,
    reserve as contract_address,
    amount/POW(10,decimals) as amount,
    onBehalfOf AS users,
    evt_tx_hash AS txs
FROM aave_v3_ethereum.Pool_evt_Supply deposits
LEFT JOIN tokens.erc20 t ON deposits.reserve = t.contract_address AND blockchain = 'ethereum' --join token metadata to get symbol and decimals
WHERE evt_block_time > NOW() - interval '{{Trading Num Days}}' DAY -- retrict data to teh selected recent time window

UNION ALL

SELECT -- Poligon (blockchain) deposits
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

SELECT -- Optimism (blockchain) deposits
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

SELECT -- Arbitrum (blockchain) deposits
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

SELECT -- Avalance (blockchain) deposits
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

SELECT -- Fantom (blochchain) deposits
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

SELECT --Base (blockchain) deposits
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

p AS ( -- build a price table to estimate USD deposit volume
SELECT
    date_trunc('{{Time interval}}', minute) as day,
    blockchain,
    contract_address,
    AVG(price) AS avg_price
FROM prices.usd
WHERE minute > NOW() - interval '{{Trading Num Days}}' DAY
GROUP BY 1, 2, 3
)

SELECT -- join deposit with token prices & aggregate final metrics by day & blockchain
    data.day,
    data.blockchain,
    SUM(amount*avg_price) AS volume,
    COUNT(DISTINCT users) AS users,
    COUNT(DISTINCT txs) AS txs
FROM data
LEFT JOIN p ON data.day = p.day AND p.contract_address = data.contract_address AND p.blockchain = data.blockchain
GROUP BY 1, 2 -- aggregate final results by time bucket & blockchain
ORDER BY 1 DESC
