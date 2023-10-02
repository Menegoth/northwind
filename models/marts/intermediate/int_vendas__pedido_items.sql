with ordens as (
        select *
        from {{ ref('stg_erp__ordens') }}
    ),

    ordem_detalhes as (
        select *
        from {{ ref('stg_erp__ordem_detalhes') }}
    ),

    join_tabelas as (
        select
            ordens.id_pedido,
            ordens.id_funcionario,
            ordens.id_cliente,
            ordens.id_transportadora,
            ordem_detalhes.id_produto,
            ordens.data_do_pedido,
            ordens.frete,
            ordens.destinatario,
            ordens.endereco_destinatario,
            ordens.cep_destinatario,
            ordens.cidade_destinatario,
            ordens.regiao_destinatario,
            ordens.pais_destinatario,
            ordens.data_do_envio,
            ordens.data_requerida_entrega,
            ordem_detalhes.desconto_perc,
            ordem_detalhes.preco_da_unidade,
            ordem_detalhes.quantidade,
        from ordem_detalhes
        left join ordens 
        on ordem_detalhes.id_pedido = ordens.id_pedido
    ),

    criar_chave as (
        select 
            cast(id_pedido as string) || cast(id_produto as string) as sk_pedido_item,
            *
        from join_tabelas
    )

select * 
from criar_chave