with
    dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    ),

    dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    ),

    pedido_itens as (
        select *
        from {{ ref('int_vendas__pedido_items') }}
    ),

    join_tabelas as (
        select
            pedido_itens.sk_pedido_item,
            pedido_itens.id_pedido,
            pedido_itens.id_funcionario,
            dim_clientes.pk_cliente,
            pedido_itens.id_transportadora,
            dim_produtos.pk_produto,
            pedido_itens.data_do_pedido,
            pedido_itens.frete,
            pedido_itens.destinatario,
            pedido_itens.endereco_destinatario,
            pedido_itens.cep_destinatario,
            pedido_itens.cidade_destinatario,
            pedido_itens.regiao_destinatario,
            pedido_itens.pais_destinatario,
            pedido_itens.data_do_envio,
            pedido_itens.data_requerida_entrega,
            pedido_itens.desconto_perc,
            pedido_itens.preco_da_unidade,
            pedido_itens.quantidade,            
            dim_produtos.nome_produto,
            dim_produtos.nome_categoria,
            dim_produtos.nome_fornecedor,
            dim_produtos.is_discontinuado,
            dim_clientes.nome_cliente

        from pedido_itens
        left join dim_produtos
        on pedido_itens.id_produto = dim_produtos.id_produto
        left join dim_clientes
        on pedido_itens.id_cliente = dim_clientes.id_cliente
    ),

    transformacoes as (
        select 
            *,
            preco_da_unidade * quantidade as total_bruto,
            (1- desconto_perc) * preco_da_unidade * quantidade as total_liquido,
            case
                when desconto_perc > 0 then true
                when desconto_perc = 0 then false
                else false
            end as is_desconto,
            frete / count(id_pedido) over (partition by id_pedido) as frete_ponderado
        from join_tabelas
    )

select * 
from transformacoes