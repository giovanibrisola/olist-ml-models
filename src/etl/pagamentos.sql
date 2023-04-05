-- Databricks notebook source
SELECT * FROM silver.olist.item_pedido AS t1

-- COMMAND ----------

WITH tb_join AS (

SELECT t2.*,
        t3.idVendedor
       
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.item_pedido AS t3
On t1.idPedido = t3.idPedido

WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t3.idVendedor IS NOT NULL

),

tb_group AS (

SELECT idVendedor, 
        descTipoPagamento,
        count(distinct idPedido) AS qtdePedidoMeioPagamento,
        sum (vlPagamento) AS vlPedidoMeioPagamento


FROM tb_join

GROUP BY idVendedor, descTipoPagamento
ORDER BY idVendedor, descTipoPagamento

)

SELECT idVendedor,

sum(case when descTipoPagamento = 'boleto'
                then qtdePedidoMeioPagamento 
                else 0 
                end) as qtde_boleto_pedido, 
sum(case when descTipoPagamento = 'credit_card'
                then qtdePedidoMeioPagamento 
                else 0 
                end) as qtde_credit_card_pedido, 
sum(case when descTipoPagamento = 'voucher'
                then qtdePedidoMeioPagamento 
                else 0 
                end) as qtde_voucher_pedido, 
sum(case when descTipoPagamento = 'debit_card'
                then qtdePedidoMeioPagamento 
                else 0 
                end) as qtde_debit_card_pedido, 
                
                
sum(case when descTipoPagamento = 'boleto'
                then qtdePedidoMeioPagamento 
                else 0 
                end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido, 
sum(case when descTipoPagamento = 'credit_card'
                then qtdePedidoMeioPagamento 
                else 0 
                end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
sum(case when descTipoPagamento = 'voucher'
                then qtdePedidoMeioPagamento 
                else 0 
                end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
sum(case when descTipoPagamento = 'debit_card'
                then qtdePedidoMeioPagamento 
                else 0 
                end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido, 
                
                
sum(case when descTipoPagamento = 'boleto'
                then vlPedidoMeioPagamento 
                else 0 
                end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido, 
sum(case when descTipoPagamento = 'credit_card'
                then vlPedidoMeioPagamento 
                else 0 
                end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
sum(case when descTipoPagamento = 'voucher'
                then vlPedidoMeioPagamento 
                else 0 
                end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido, 
sum(case when descTipoPagamento = 'debit_card'
                then vlPedidoMeioPagamento 
                else 0 
                end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido


  FROM tb_group

  GROUP BY 1
