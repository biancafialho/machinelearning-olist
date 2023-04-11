WITH tb_pedidos AS (

  SELECT 
      DISTINCT 
      t1.idPedido,
      t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'
  AND t1.dtPedido >= add_months('{date}', -6)
  AND idVendedor IS NOT NULL

),
tb_join AS (
SELECT t2.*,
       t1.idVendedor
       
FROM tb_pedidos AS t1

LEFT JOIN silver.olist.pagamento_pedido as t2
ON t1.idPedido = t2.idPedido

),

tb_group AS (
SELECT idVendedor,
       descTipoPagamento,
       count(distinct idPedido) as qtPedidoMeioPagamento,
       sum (vlPagamento) as vlPedidoMeioPagamento
       
FROM tb_join

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento
),

tb_summary AS (

SELECT idVendedor,

-- Pivot calculando as quantidades

sum(CASE WHEN descTipoPagamento = 'boleto' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN qtPedidoMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,

sum(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,

-- Para calcular proporção 

sum(CASE WHEN descTipoPagamento = 'boleto' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN qtPedidoMeioPagamento ELSE 0 END) / sum(qtPedidoMeioPagamento)AS pct_qtde_debit_card_pedido,

sum(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
sum(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
sum(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
sum(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/sum(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido

FROM tb_group

GROUP BY idVendedor
),

-----
tb_cartao as (

  SELECT idVendedor,
         AVG(nrParcelas) AS avgQtdeParcelas,
         PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
         MAX(nrParcelas) AS maxQtdeParcelas,
         MIN(nrParcelas) AS minQtdeParcelas

  FROM tb_join

  WHERE descTipoPagamento = 'credit_card'

  GROUP BY idVendedor

)

SELECT 
       '{date}' AS dtReference,
       NOW() AS dtIngestion,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas

FROM tb_summary as t1

LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor
